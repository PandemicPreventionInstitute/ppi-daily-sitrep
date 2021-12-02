# -*- coding: utf-8 -*-
"""
Created on Wed Nov 17 16:55:21 2021

@author: zsusswein
"""

from datetime import date
import pandas as pd
import process_nextstrain_exclude
from datetime import date, timedelta, datetime


def check_col_names(gisaid_df: pd.core.frame.DataFrame) -> bool:
    
    expected_cols = {'Virus name', 'Type', 'Accession ID', 'Collection date',
                     'Location','Additional location information', 
                     'Sequence length', 'Host',
       'Patient age', 'Gender', 'Clade', 'Pango lineage', 'Pangolin version',
       'Variant', 'AA Substitutions', 'Submission date', 'Is reference?',
       'Is complete?', 'Is high coverage?', 'Is low coverage?', 'N-Content',
       'GC-Content'}
    
    observed_cols = set(gisaid_df.columns)
    
    difference = expected_cols - observed_cols
    
    try:
        len(difference) == 0

    except:
        raise RuntimeError('Missing column(s): %s' % difference)


def is_suspect_date(collection_date: str) -> bool:
    """Check that the date is feasible.

    This function checks that the date is at earliest Dec. 1, 2019, at latest
    today, and formatted in the expected way (at least year-month).
    
    TODO: read in prior metadata with this flag and pull it forward because
    suspect dates that were once identifiable as in the future will no longer
    be so at some later date

    Parameters
    ----------
    collection_date : str
        The date the sample was collected from the individual

    Returns
    -------
    legit_date : bool
        A boolean flag, True if the date is feasible
    """
    today_str = date.today().strftime('%Y-%m-%d')
    legit_date = (len(collection_date) == 10) and \
        (collection_date > '2019-12-01') and \
        (collection_date <= today_str)

    return not legit_date


def is_suspect_sequence(gisaid_df: pd.core.frame.DataFrame) -> bool:
    """Check that sequence meets minimum quality standards.

    To be "quality", a sequence must be at least 20000 bps, have at least 3/4
    of the bps identified, and have been sampled from a human. Is meant to
    be passed by row (i.e. pd.apply(axis = 1)) and so returns a boolean
    pd.series object.

    Parameters
    ----------
    gisaid_df : pandas.core.frame.DataFrame
        The dataframe containing GISAID metadata.

    Returns
    -------
    quality_sequence : bool
        A boolean flag, True if the sequence meets minimum standards.

    """
    quality_sequence = (gisaid_df['Sequence length'] > 20000) & \
        ((gisaid_df['N-Content'] < 0.02) | pd.isna(gisaid_df['N-Content'] )) & \
        (gisaid_df['Host'] == 'Human')

    return ~quality_sequence

def is_abnormal_gc_content(gisaid_df: pd.core.frame.DataFrame) -> bool:
    """
    flag abormal GC content in sequence, a marker of sequencing errors

    Parameters
    ----------
    gisaid_df : pd.core.frame.DataFrame
        The dataframe containing GISAID metadata.

    Returns
    -------
    bool
        A boolean flag, True if GC content is normal.

    """
    
    normal = (gisaid_df['GC-Content'] < 4e-1) & \
        (gisaid_df['GC-Content'] > 25e-2)
    
    return ~normal

def flag_suspect_sequences(gisaid_df):
    """
    Generate columns with boolean flags for suspect sequences

    Parameters
    ----------
    gisaid_df : pd.core.frame.DataFrame
        The dataframe containing GISAID metadata.

    Returns
    -------
    gisaid_df : pd.core.frame.DataFrame
        The dataframe containing GISAID metadata with flag cols added.

    """
    
    # load and filter sequences on Nextstrain exclude list
    exclude_sequences_response = process_nextstrain_exclude.load_nextstrain_exclude_sequences()
    exclude_sequences = process_nextstrain_exclude.process_nextstrain_exclude_sequences(exclude_sequences_response)

    gisaid_df['nextstrain_excluded'] = process_nextstrain_exclude.is_nextstrain_exclude_sequence(gisaid_df, exclude_sequences)
    gisaid_df['abnormal_date'] = gisaid_df['Collection date'].apply(is_suspect_date)
    gisaid_df['suspect_sequence'] = is_suspect_sequence(gisaid_df)
    gisaid_df['abnormal_GC_content'] = is_abnormal_gc_content(gisaid_df)
    
    gisaid_df['any_abnormal'] = (gisaid_df['nextstrain_excluded'] +  \
        gisaid_df['abnormal_date'] + gisaid_df['suspect_sequence'] +  \
        gisaid_df['abnormal_GC_content']) > 0

    return gisaid_df

def get_weekstartdate(dt_value):
    start = dt_value - timedelta(days=dt_value.weekday())
    return start

def titlecase_location(location_name, exceptions=['and', 'or', 'the', 'a', 'of', 'in', "d'Ivoire"]):
    word_list = [word if word in exceptions else word.capitalize() for word in location_name.split(' ')]
    return ' '.join(word_list)

def correct_location_names(gisaid_df):
    gisaid_df.loc[:,'country'] = gisaid_df['country'].apply(titlecase_location)
    gisaid_df.loc[gisaid_df['country'].fillna('').str.contains('USA', case=False), 'country'] = 'United States'
    gisaid_df.loc[gisaid_df['country'] == 'Puerto Rico', 'country'] = 'United States'
    gisaid_df.loc[gisaid_df['country'] == 'Guam', 'country'] = 'United States'
    gisaid_df.loc[gisaid_df['country'] == 'Northern Mariana Islands', 'country'] = 'United States'
    gisaid_df.loc[gisaid_df['country'] == 'U.s. Virgin Islands', 'country'] = 'United States'
    gisaid_df.loc[gisaid_df['country'] == 'Czech Republic', 'country'] = 'Czechia'
    gisaid_df.loc[gisaid_df['country'] == 'Antigua', 'country'] = 'Antigua and Barbuda'
    gisaid_df.loc[gisaid_df['country'] == 'Democratic Republic of the Congo', 'country'] = 'Democratic Republic of Congo'
    gisaid_df.loc[gisaid_df['country'] == 'Republic of the Congo', 'country'] = 'Congo'
    gisaid_df.loc[gisaid_df['country'] == 'Faroe Islands', 'country'] = 'Faeroe Islands'
    gisaid_df.loc[gisaid_df['country'] == 'Guinea Bissau', 'country'] = 'Guinea-Bissau'
    gisaid_df.loc[gisaid_df['country'] == 'Niogeria', 'country'] = 'Nigeria'
    gisaid_df.loc[gisaid_df['country'] == 'Bosni and Herzegovina', 'country'] = 'Bosnia and Herzegovina'
    gisaid_df.loc[gisaid_df['country'] == 'England', 'country'] = 'United Kingdom'
    gisaid_df.loc[gisaid_df['country'] == 'The Bahamas', 'country'] = 'Bahamas'
    return gisaid_df

def annotate_sequences(gisaid_df):
    gisaid_df['region'] = gisaid_df.Location.apply(lambda x: x.split('/')[0].strip())
    gisaid_df['country'] = gisaid_df.Location.apply(lambda x: x.split('/')[1].strip())
    gisaid_df['division'] = gisaid_df.Location.apply(
        lambda x: x.split('/')[2].strip() if len(x.split('/'))>2 else '')

    # replace 'USA' string with 'United States' etc in location, to match OWID location name
    gisaid_df = correct_location_names(gisaid_df)

    gisaid_df['collect_date'] = pd.to_datetime(gisaid_df['Collection date'])
    gisaid_df['submit_date'] = pd.to_datetime(gisaid_df['Submission date'])

    gisaid_df['lag_days'] = gisaid_df['submit_date'] - gisaid_df['collect_date']
    gisaid_df['lag_days'] = gisaid_df['lag_days'].dt.days.astype('int')

    # using ISO 8601 year and week (Monday as the first day of the week. Week 01 is the week containing Jan 4)
    gisaid_df['collect_yearweek'] = gisaid_df['collect_date'].apply(lambda x: datetime.strftime(x, "%G-W%V"))
    gisaid_df['submit_yearweek'] = gisaid_df['submit_date'].apply(lambda x: datetime.strftime(x, "%G-W%V"))

    gisaid_df['collect_weekstartdate'] = gisaid_df['collect_date'].apply(get_weekstartdate)
    gisaid_df['submit_weekstartdate'] = gisaid_df['submit_date'].apply(get_weekstartdate)

    return gisaid_df

def subset_gisaid_df(gisaid_df):
    cols = ['Collection date','Accession ID','Pango lineage',
            'Location','region','country','division',
            'collect_date', 'submit_date', 'lag_days',
            'collect_yearweek','collect_weekstartdate',
            'submit_yearweek','submit_weekstartdate',
             'nextstrain_excluded', 'abnormal_date',
       'suspect_sequence', 'abnormal_GC_content','any_abnormal']
    return gisaid_df[cols]


def process_raw_metadata(gisaid_df):
    
    check_col_names(gisaid_df)
    
    gisaid_df = flag_suspect_sequences(gisaid_df)
    gisaid_df = annotate_sequences(gisaid_df)
    gisaid_df = subset_gisaid_df(gisaid_df)
    
    return gisaid_df



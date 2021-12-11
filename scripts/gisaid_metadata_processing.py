# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 13:47:38 2021.

@author: zsusswein


Takes a file called metadata.tsv in the data folder

Processes GISAID metadata to generate a digested set of data files, a
nd merges with population and case counts from Our World Of Data.

These are the rules we're using to filter GISAID sequences:
    - at least 20000 in length
    - collection date must not be in the future and must be at the 
    granularity of year/month/day, earliest Dec 2019
    - excluding sequences from the Nextstrain exclude list
    - only human samples

Filters for future consideration (not doing these yet):
    - excluding sequences with greater than 5% ambiguous base calls (N)
    
"""

import datetime
from datetime import date, timedelta
import sys
import numpy as np
import pandas as pd
import requests
from filter_gisaid_metadata import process_raw_metadata, get_weekstartdate

##############################################################################################
####################   Designate variants for breakout columns    ############################
##############################################################################################

# greek naming from WHO (https://www.who.int/en/activities/tracking-SARS-CoV-2-variants/)
# cross-referencing CDC (https://www.cdc.gov/coronavirus/2019-ncov/variants/variant-info.html) 
# and Outbreak.info (https://outbreak.info/situation-reports) 
# and CoVariants.org (https://covariants.org/variants)
# names are created as new cols verbatim in the weeky csv with value being the sum of sequence counts for the pango lineages in each corresponding value plus a 'who_other' column that covers the remaining sequences not designated below

greek_dict = {
    'who_alpha' :  ['B.1.1.7','Q.*'],
    'who_beta' :   ['B.1.351', 'B.1.351.2', 'B.1.351.3', 'B.1.351.4', 'B.1.351.5'],
    'who_gamma' :  ['P.1', 'P.1.*'],
    'who_delta':   ['B.1.617.2', 'AY.*'],
    'who_omicron': ['B.1.1.529', 'B.1.1.529.*', 'BA.*'],
    'who_allvois': ['C.37', 'C.37.1', # lambda
                    'B.1.621', 'B.1.621.1', # mu
                    ],
}

# every pango lineage in the following lists are broken out as new columns verbatim plus an "All lineages" column as the sum of all sequences and "Other lineages" being "All lineages" minus these designated variant counts
vocs = greek_dict['who_alpha']\
        + greek_dict['who_beta']\
        + greek_dict['who_gamma']\
        + greek_dict['who_delta'] \
        +greek_dict['who_omicron']

vois = greek_dict['who_allvois']

other_important =  ['B.1.617.3', # CDC VUM
                    'B.1.427', 'B.1.429', 'B.1.427/429', # epsilon WHO VUM
                    'B.1.525', # eta WHO VUM
                    'B.1.526', 'B.1.526.*', # iota WHO VUM
                    'B.1.617.1', # kappa WHO VUM
                    'P.2', # zeta
                    'P.3', # theta
                    ]

# if needed, combine separate lineages under one column. Note that this supersedes the lineage breakout column designations above, i.e. B.1.427 and B.1.429 are collapsed into one column named `B.1.427/429`
lineage_replace_dict = {
    'B.1.427':'B.1.427/429',
    'B.1.429':'B.1.427/429',   
}

##############################################################################################
######################################   GISAID data load    #################################
##############################################################################################

def find_lineages(input_pango, search_pango):
    # retrieve the pango lineages that exist in the latest gisaid set including sublineages wildcarded with *, i.e. "AY.*"
    match_list = sorted(set(search_pango) & set(input_pango))
    print('  Matched these lineages:', match_list)
    wildcards = [c for c in input_pango if '*' in c]
    wildcard_res = []
    for w in wildcards:
        res = [c for c in set(search_pango) if w.replace('*','') == str(c)[:len(w)-1]]
        print(f'  Found {w}:',sorted(res))
        wildcard_res += res
    not_found = set(input_pango)-set(wildcards)-set(match_list)
    if len(not_found)>0: print('  Not found:', not_found)
    return sorted(match_list+wildcard_res)

def aggregate_with_lineage(gisaid_df):
    country_variants_df = gisaid_df.groupby(
        ['collect_date','collect_yearweek','collect_weekstartdate','country','Pango lineage']).count()[['Accession ID']].reset_index()
    # find pango lineages that are actually present in gisaid metadata, including wildcards
    key_variants = find_lineages(vocs + vois + other_important, gisaid_df['Pango lineage'].unique())
    # label these lineages of interest for breaking out into cols
    country_variants_df['key_lineages'] = country_variants_df['Pango lineage'].apply(
        lambda x: x if x in key_variants else 'Other lineages')
    # manually combine lineages, i.e. B.1.427 and B.1.429 under B.1.427/429
    country_variants_df['key_lineages'].replace(lineage_replace_dict, inplace=True)
    
    all_sequences = gisaid_df.groupby(
        ['collect_date','collect_yearweek','collect_weekstartdate','country']).count()[['Accession ID']].reset_index()
    all_sequences['key_lineages'] = 'All lineages'
    country_variants_df = pd.concat([country_variants_df, all_sequences], sort=True)
 
    # rename columns a bit
    country_variants_df.columns = ['_'.join(c.lower().split()) for c in country_variants_df.columns]
    return country_variants_df

def calc_lagstats(gisaid_df, group_cols=['collect_date','country']):
    # precalculate summary stats about lag time from date_collect to date_submit for all filtered sequences per day and country
    def q1(x): return x.quantile(0.25)
    def q3(x): return x.quantile(0.75)
    sumstats_df = gisaid_df.groupby(group_cols).agg({'lag_days':['count','median','min','max',q1,q3]}).reset_index()
    sumstats_df.rename(columns={'count':'seq_count',
                       'median':'gisaid_lagdays_median',
                       'q1':'gisaid_lagdays_q1',
                       'q3':'gisaid_lagdays_q3',
                       'min':'gisaid_lagdays_min',
                       'max':'gisaid_lagdays_max',
                       'lag_days':'',
                       }, inplace=True)
    sumstats_df.columns = sumstats_df.columns.map(''.join)
    return sumstats_df

##############################################################################################
######################################   OWID data load    ###################################
##############################################################################################

def load_owid_df():
    url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv'
    owid_df = pd.read_csv(url, parse_dates=['date'])
    owid_df.sort_values(['location','date'], ascending=True, inplace=True)
    # only keep data after Dec 2019
    owid_df = owid_df[owid_df['date']>='2019-12-01']

    # drop owid region rows which start with OWID_ except specific locations
    owid_df = owid_df[~owid_df['iso_code'].isin([
                                                'OWID_AFR', # Africa
                                                'OWID_ASI', # Asia
                                                'OWID_EUR', # Europe
                                                'OWID_EUN', # European Union
                                                'OWID_INT', # International
                                                # 'OWID_KOS', # Kosovo
                                                'OWID_NAM', # North America
                                                # 'OWID_CYN', # North Cyprus
                                                'OWID_OCE', # Oceania
                                                'OWID_SAM', # South America
                                                'OWID_WRL', # World
                                                ])]

    # subset of columns, prepend 'owid_' to each column name
    owid_cols = ['date','location','iso_code','continent','new_cases','new_cases_smoothed','population','people_vaccinated','people_fully_vaccinated']
    owid_df = owid_df[owid_cols]
    
    # add vax columns calculating the daily change in people vaccinated to roll up into weekly sums
    for loc in owid_df.location.unique():
        owid_df.loc[owid_df['location']==loc,'new_people_vaccinated'] = owid_df[owid_df['location']==loc]['people_vaccinated'].ffill().fillna(0).diff()
        owid_df.loc[owid_df['location']==loc,'new_people_fully_vaccinated'] = owid_df[owid_df['location']==loc]['people_fully_vaccinated'].ffill().fillna(0).diff()        

    owid_df.columns = ['owid_%s' % x for x in owid_df.columns]
    
    return owid_df


##############################################################################################
##################################      Merge and pivot data   ###############################
##############################################################################################

def merge_gisaid_owid(country_variants_df, owid_df):    
    merged_df = pd.merge(country_variants_df, owid_df,
        how='outer',
        left_on=['collect_date', 'country'],
        right_on=['owid_date', 'owid_location'],
    )

    # copy missing values from OWID data to GISAID columns and vice versa as necessary (GISAID will lag)
    merged_df['collect_date']= np.where(
        merged_df['collect_date'].isnull(), merged_df['owid_date'], merged_df['collect_date'])
    merged_df['owid_date']= np.where(
        merged_df['owid_date'].isnull(), merged_df['collect_date'], merged_df['owid_date'])
    merged_df['country']= np.where(
        merged_df['country'].isnull(), merged_df['owid_location'], merged_df['country'])
    
    return merged_df

def pivot_merged_df(merged_df):
  # # add a placeholder in order to pivot on key_lineages and not drop empty collect_date rows
    merged_df['key_lineages'] = merged_df['key_lineages'].fillna('placeholder_dropmeplease')
    
    country_variants_pivot = merged_df.pivot_table(
        values=['accession_id'], aggfunc='sum', dropna=True, index=['collect_date','country'],
        columns=['key_lineages']).droplevel(axis=1, level=0).reset_index()

    # reorganize col order
    country_variants_pivot = country_variants_pivot[['collect_date','country','placeholder_dropmeplease','All lineages']+sorted([c for c in country_variants_pivot.columns if '.' in c])+['Other lineages']]

    # merge in owid cases columns which are date-dependent
    cols = ['owid_location', 'owid_date', 'owid_new_cases', 'owid_new_cases_smoothed','owid_new_people_vaccinated','owid_new_people_fully_vaccinated']
    country_variants_all_lineages = merged_df[
        merged_df['key_lineages'].isin(['All lineages','placeholder_dropmeplease'])][cols]
    country_variants_pivot = pd.merge(
        country_variants_pivot, country_variants_all_lineages, 
        how='left',
        left_on=['country','collect_date'],
        right_on=['owid_location','owid_date'],
    )

    # merge in owid population regardless of date
    country_variants_pivot = pd.merge(
        country_variants_pivot, 
        merged_df[merged_df['key_lineages'].isin(['All lineages','placeholder_dropmeplease'])][['owid_location', 'owid_continent', 'owid_population']].drop_duplicates(),
        how='left',
        left_on=['country'],
        right_on=['owid_location'],
        suffixes=('_drop',''),
    )
    # drop the original owid_location col which is sparser across the timeseries
    country_variants_pivot.drop('owid_location_drop', axis=1, inplace=True)

    # copy over collect_date where owid_date is missing, otherwise owid_date is NaT which gets filtered out
    country_variants_pivot['owid_date']= np.where(
        country_variants_pivot['owid_date'].isnull(), country_variants_pivot['collect_date'], country_variants_pivot['owid_date'])
    country_variants_pivot.sort_values(
        ['owid_date','owid_location'], ascending=[True, True], inplace=True)

    # fill out the yearweek and weekstartdate missing value
    country_variants_pivot['collect_yearweek'] = country_variants_pivot['collect_date'].apply(lambda x: datetime.datetime.strftime(x, "%G-W%V"))
    country_variants_pivot['collect_weekstartdate'] = country_variants_pivot['collect_date'].apply(get_weekstartdate)

    country_variants_pivot.drop('placeholder_dropmeplease', axis=1, inplace=True)

    return country_variants_pivot

# local path
# def add_regions(merged_df, region_path='../data/raw/who_regions (2).csv'):
# domino path
def add_regions(merged_df, region_path='/mnt/data/raw/who_regions (2).csv'):
    who_regions = pd.read_csv(region_path)
    merged_df = pd.merge(merged_df, who_regions[['Entity','WHO region']], how='left', left_on=['owid_location'], right_on=['Entity'])
    merged_df.rename(columns={'WHO region': 'who_region'}, inplace=True)
    merged_df.drop('Entity', axis=1, inplace=True)
    return merged_df

# OWID dataset already has a continent column so using that instead
# def add_continents(merged_df, region_path='data/continents-according-to-our-world-in-data.csv'):
#     continents = pd.read_csv(region_path)
#     merged_df = pd.merge(merged_df, continents[['Entity','Continent']], how='left', left_on=['owid_location'], right_on=['Entity'])
#     merged_df.rename(columns={'Continent': 'owid_continent'}, inplace=True)
#     merged_df.drop('Entity', axis=1, inplace=True)
#     return merged_df    

def concat_agglocations(merged_pivoted_df, group_cols=['collect_date','collect_yearweek','collect_weekstartdate','owid_date']):
    continents_df = merged_pivoted_df.groupby(group_cols+['owid_continent']).sum().reset_index()
    whoregions_df = merged_pivoted_df.groupby(group_cols+['who_region']).sum().reset_index()
    global_df = merged_pivoted_df.groupby(group_cols).sum().reset_index()
    
    # create new col to distinguish these from country-level rows
    continents_df.loc[:,'aggregate_location'] = continents_df['owid_continent']
    whoregions_df.loc[:,'aggregate_location'] = 'WHO Region: ' + whoregions_df['who_region']
    global_df.loc[:,'aggregate_location'] = 'Global'
    
    agglocation_df = pd.concat([continents_df, whoregions_df, global_df], sort=False)

    return agglocation_df

def calc_regional_lagstats(gisaid_owid_df, group_cols=['collect_weekstartdate']):
    continents_sumstats_df = calc_lagstats(gisaid_owid_df, group_cols=group_cols+['owid_continent'])
    whoregions_sumstats_df = calc_lagstats(gisaid_owid_df, group_cols=group_cols+['who_region'])
    global_sumstats_df = calc_lagstats(gisaid_owid_df, group_cols=group_cols)

    continents_sumstats_df.loc[:,'aggregate_location'] = continents_sumstats_df['owid_continent']
    whoregions_sumstats_df.loc[:,'aggregate_location'] = 'WHO Region: '+ whoregions_sumstats_df['who_region']
    global_sumstats_df.loc[:,'aggregate_location'] = 'Global'

    return pd.concat([whoregions_sumstats_df, continents_sumstats_df, global_sumstats_df], sort=False)

def cleanup_columns(merged_df, gisaid_cols):
    # prepend gisaid_ to respective columns except for the lineage ones
    renamed_cols = {c:'gisaid_'+c for c in merged_df.columns if (c in gisaid_cols)}
    merged_df.rename(columns=renamed_cols, inplace=True)
    return merged_df

def calc_vax_bottomup(vax_df, loc_col = 'owid_location'):
    # adding up the daily or weekly new people vaccinated per region and then calculating the percent of pop
    for loc in vax_df[loc_col].unique():
      vax_df.loc[vax_df[loc_col]==loc,'owid_people_vaccinated'] = vax_df[vax_df[loc_col]==loc]['owid_new_people_vaccinated'].cumsum()
      vax_df.loc[vax_df[loc_col]==loc,'owid_people_fully_vaccinated'] = vax_df[vax_df[loc_col]==loc]['owid_new_people_fully_vaccinated'].cumsum()
    
    vax_df['owid_people_vaccinated_per_hundred'] = np.round(vax_df['owid_people_vaccinated'] / (vax_df['owid_population']/100),2)
    vax_df['owid_people_fully_vaccinated_per_hundred'] = np.round(vax_df['owid_people_fully_vaccinated'] / (vax_df['owid_population']/100),2)
    return vax_df

def get_owid_vax_regional(get_weekly=True):
    iso2loc_dict = {
        'OWID_AFR':'Africa',
        'OWID_ASI':'Asia',
        'OWID_EUR':'Europe',
        'OWID_NAM':'North America',
        'OWID_OCE':'Oceania',
        'OWID_SAM':'South America',
        'OWID_WRL':'Global',
    }
    owid_url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv'
    owid_vax_df = pd.read_csv(owid_url, parse_dates=['date'])[['iso_code','date','people_vaccinated_per_hundred','people_fully_vaccinated_per_hundred']]
    owid_vax_df.columns = ['owid_%s' % x for x in owid_vax_df.columns]
    owid_vax_df['aggregate_location'] = owid_vax_df['owid_iso_code'].map(iso2loc_dict)
    owid_vax_df['gisaid_collect_weekstartdate'] = owid_vax_df['owid_date'].apply(get_weekstartdate)
    owid_vax_df.drop('owid_iso_code', axis=1, inplace=True)

    region_vax_df = owid_vax_df[(~owid_vax_df['aggregate_location'].isna())]
    
    # only use data from the last day of each week to represent the week's progress
    if get_weekly: region_vax_df = region_vax_df[(region_vax_df['owid_date'].dt.weekday==6)]
    
    return region_vax_df

def overwrite_vax_regional(df):
    # overwrite these calculated fields at the continent and global level with OWID reported values because of discrepancy vs the bottom-up calcs
    overwrite_cols = ['owid_people_vaccinated_per_hundred','owid_people_fully_vaccinated_per_hundred']
    regional_vax_weekly_df = get_owid_vax_regional()
    overwrite_locations = regional_vax_weekly_df['aggregate_location'].unique()
    
    df.loc[df['aggregate_location'].isin(overwrite_locations), overwrite_cols] = np.nan
    df.set_index(['aggregate_location','gisaid_collect_weekstartdate'], inplace=True)
    regional_vax_weekly_df.set_index(['aggregate_location','gisaid_collect_weekstartdate'], inplace=True)
    for col in overwrite_cols: 
        df[col].update(regional_vax_weekly_df[col])
    df.reset_index(inplace=True)
    return df

def calculate_cols(df):
    df['sequences_over_new_cases'] = df['All lineages'] / df['owid_new_cases']
    df.replace(np.inf, np.nan, inplace=True)
    df['new_cases_per_mil'] = df['owid_new_cases'] / (df['owid_population']/1e6)
    return df

def aggregate_weekly(df):
    df = df[[c for c in df.columns if 'lagdays' not in c]] # drop the lagday summary stat cols, need to recompute these
    weekly_agg_df = df.groupby(['gisaid_collect_weekstartdate','gisaid_collect_yearweek','gisaid_country']).sum().reset_index()
    weekly_agg_df.drop(['owid_population','owid_new_cases_smoothed'], axis=1, inplace=True)
    weekly_agg_df = pd.merge(weekly_agg_df, df[['owid_location','owid_population','who_region','owid_continent']].drop_duplicates(), 
                            how='left', left_on=['gisaid_country'], right_on=['owid_location'])
    return weekly_agg_df

def add_greek_cols(df):
    # break out new columns aggregating the counts of variants grouped under WHO greek letters designated in greek_dict at top
    # first find the lineages that exist in this gisaid export
    greek_lineages_flattened = [v for v in greek_dict.values() for v in v]
    match_list = find_lineages(greek_lineages_flattened, set(df.columns))
    
    # group lineages under each greek letter designation, sum, and create new 'who_' col for them
    print('Grouping into greek cols:','\n--------------------------')
    who_list = []
    for k, v in greek_dict.items(): 
        print(f'Finding and grouping lineages for {k}:')
        v_existing = find_lineages(v, set(df.columns))
        df[k] = df[v_existing].sum(axis=1)
        print('-->',k,':',v_existing)
        who_list += v_existing
    df['who_other'] = df['All lineages'] - df[match_list].sum(axis=1)
    return df

def main(args_list=None):

    # local path
    # gisaid_df = pd.read_csv('../data/raw/metadata.tsv', sep='\t')

    # domino path
    gisaid_df = pd.read_csv('/domino/datasets/local/metadata/metadata.tsv', sep='\t')

    print('Loading and filtering GISAID data...')
    gisaid_df = process_raw_metadata(gisaid_df)
    gisaid_cols = list(gisaid_df.columns)
    print('Done, %d sequences' % gisaid_df.shape[0])
    
    gisaid_df_subset = gisaid_df[['collect_date', 'submit_date', 'any_abnormal', 'country', 'Pango lineage']]

    # local path
    # gisaid_df_subset.to_csv('../data/processed/inital_clean_metadata.csv')

    # domino path
    gisaid_df_subset.to_csv('/mnt/data/processed/inital_clean_metadata.csv')

    print('Aggregating GISAID data...')
    print('Break out key pango lineages into columns')
    gisaid_country_variants_df = aggregate_with_lineage(gisaid_df)
    print('Done.')

    print('Loading OWID data...')
    owid_df = load_owid_df()
    print('Done, %d rows' % owid_df.shape[0])

    print('Merging GISAID and OWID data...')
    merged_df = merge_gisaid_owid(gisaid_country_variants_df, owid_df)
    print('Pivoting merged data...')
    merged_pivoted_df = pivot_merged_df(merged_df)
    print('Add region assignments to countries...')
    merged_pivoted_df = add_regions(merged_pivoted_df)
    # merged_pivoted_df = add_continents(merged_pivoted_df)
    print('Aggregate locations and concatenate...')
    agglocation_df = concat_agglocations(merged_pivoted_df)
    merged_pivoted_df = pd.concat([merged_pivoted_df, agglocation_df], sort=False)
    print('Add submission lag stats...')
    sumstats_df = calc_lagstats(gisaid_df)  
    merged_pivoted_df = pd.merge(merged_pivoted_df, sumstats_df, how='left')
    print('Final data file cleanup...')
    merged_pivoted_df = cleanup_columns(merged_pivoted_df, gisaid_cols)
    #print(f'Locations without OWID join and how many sequences:\n{merged_pivoted_df[(merged_pivoted_df["owid_location"].isna())&(merged_pivoted_df["aggregate_location"].isna())].groupby("gisaid_country").sum()["All lineages"]}')
    print('Done.')

    max_gisaid_date = gisaid_df.submit_date.max()
    merged_pivoted_df_latest = merged_pivoted_df.loc[(merged_pivoted_df.owid_date <= max_gisaid_date)]
    # local path
    # merged_pivoted_df_latest.to_csv('../data/processed/gisaid_cleaning_output.csv')
    # domino path
    merged_pivoted_df_latest.to_csv('/mnt/data/processed/gisaid_cleaning_output.csv')

if __name__ == "__main__":
    main()
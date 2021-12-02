# -*- coding: utf-8 -*-
"""
Created on Wed Nov 17 12:16:57 2021.

@author: zsusswein

Pulls and processes the Nextstrain sequences to exclude from GISAID in order
to clean the GISAID metadata. Adapted by original file by Dave Luo from PTC.
"""

import requests
import pandas as pd

def load_nextstrain_exclude_sequences() -> requests.models.Response:
    """Pull list of GISAID sequences identified as problematic by Nextstrain.

    Best documentation I can find on cxclusion criteria is here:
    https://docs.nextstrain.org/projects/ncov/en/latest/analysis/data-prep.html

    Returns
    -------
    exclude_sequences_response : requests.models.Response object
        Contains the exclude.txt values from the Nextstrain Github and
        information about the status of the connection request.
    """
    url = 'https://raw.githubusercontent.com/nextstrain/ncov/master/defaults/exclude.txt'
    exclude_sequences_response = requests.get(url)

    return exclude_sequences_response


def process_nextstrain_exclude_sequences(exclude_sequences_response:
                                         requests.models.Response) -> set:
    """Convert Response object to set compatible with GISAID (meta)data.

    Parameters
    ----------
    exclude_sequences_response : requests.models.Response
        Contains the exclude.txt values from the Nextstrain Github and
        information about the status of the connection request.

    Returns
    -------
    exclude_sequences : set
        The names of the sequences in the GISAID repository to be excluded
        prepended with hcov-19/ to allow compatability with GISAID structure.
    """
    exclude_sequences = {
        "hCoV-19/%s" % x for x in
        exclude_sequences_response.text.split('\n') if x != ''
        and not x.startswith('#')}

    return exclude_sequences


def is_nextstrain_exclude_sequence(gisaid_df: pd.core.frame.DataFrame,
                                   exclude_sequences: set) -> bool:
    """Check if GISAID sequence is in Nextstrain exclude list.

    Parameters
    ----------
    gisaid_df : pandas.core.frame.DataFrame
        The dataframe containing GISAID metadata.
    exclude_sequences : set
        The names of the sequences in the GISAID repository to be excluded
        prepended with hcov-19/ to allow compatability with GISAID structure.

    Returns
    -------
    exclude : bool
        A boolean flag indicating if the sequence is in the exclude list.

    """
    exclude = gisaid_df['Virus name'].isin(exclude_sequences)

    return exclude

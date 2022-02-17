# PPI Daily Situation Report Workflow

###  GISAID metadata
- Run auto_extract_gisaid_omicron.R
- This reads in the metadata feed which contains location, submission date, collection date, and pango lineage assignment of each submission
- Generates a metadata.csv in a similar format (but with reduced columns) as the GISAID COVID-19 metadata download


### Merging GISAID sequencing with case data

- Run gisaid_metadata_processing.R which aggregates the line list data, group by collection date and country.
- Merges it with the cases reported by country and day in Our World in Data
- Generates a dataset called gisaid_owid_merged.csv

### Bring the two GISAID data sources together plus FIND data on testing
- Run make_flourish_datamframe_BA2.R
- Loads in gisaid_owid_merged.csv, shapefile for Flourish regions, and lat long data for Flourish points
- Read in path for FIND data from OWID data directly
- Merges to country-level metrics over different time periods:
  n_ba_2 = number of total BA.2 cases sequenced and submitted by that country, over the whole pandemic
  n_ba_2_NA = same as n_ba_2, but 0s contain NAs for Flourish viz
  ba_2_detected = Yes if n_ba_2 >0, No if n_ba_2 <0
  cases_per_100k_last_7_days = sum of OWID cases in the past 7 days by country per 100k
  percent_of_cases_sequenced_last_30_days = sum of sequences by collection date in past 30 days/ sum of OWID cases reported in past 30 days, by country
  rounded_pct_cases_seq = same as above but rounded
  pct_cases_seq_w_NA = same as above but 
  sequences_in_last_30_days = sum of sequences by collection date in past 30 days 
  pct_seq_BA_2_last_week = starting 7 days prior, and looking another 7 days back, of the sequences collected (and already submitted) during this time, what % were BA.2
  pct_BA2_dots = same as above but 0 if there were none in the past week but BA.2 has been detected, and NA if it has yet to have been detetected
  pct_BA2_panel = same as above, but if there were no sequences collected and submitted last week, indicate so, and if there were less than 100, put an asterisk
  total_seq_last_week = number of total sequences collected and submitted starting 7 days prior and looking another 7 days back
- Exports to ppi-output repo /data/processed/gisaid_summary_df_BA2.csv 


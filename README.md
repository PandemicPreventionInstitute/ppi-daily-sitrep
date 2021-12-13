# PPI Daily Situation Report

## Workflow for pulling GISAID metadata for [daily report](https://rockfound.app.box.com/integrations/googledss/openGoogleEditor?fileId=890672848592&trackingId=3#)

### Omicron cumulative cases

- Login to GISAID
- EpiCov > Search > Variants: B.1.1.529, Clade:check > Download: Dates and Location: Downloads “gisaid_hcov-19_date_hour.tsv”
- Put this into the data/raw/
- Run summarize_metadata_to_country_day.R 
- Generates  and stores in data/processed/metadata_processed.csv

### Omicron cases from BNO
- Run scrape_BNO_table_w_date.R
- Generates and stores in data/raw/daily_BNO_file/DATE.csv # current cumulative numbers
- Generates and stores in data/raw/BNO_scraped_master.csv # adds on all new runs of the scraper with a timestamp

### GISAID sequencing/testing/cases data

- Login to GISAID
- EpiCov > Downloads > metadata > agree > download
- Unzip XXX file, add metadata.tsv to data/raw folder
- Run gisaid_metadata_processing.py 
- Generates and stores in data/processed/gisaid_cleaning_output.csv

### Bring the two GISAID data sources together plus FIND data on testing
- Load in gisaid_cleaning_output.csv, metadata_processed.csv, BNO_table.csv
- Read in path for FIND data from github directly
- Merges to country-level metrics combined with omicron sequence counts
- Export /data/processed/gisaid_summary_df.csv 
- Export /data/processed/omicron_daily_topline.csv # sums over all countries and calculates increase in sequences in GISAID and overall countries reporting.

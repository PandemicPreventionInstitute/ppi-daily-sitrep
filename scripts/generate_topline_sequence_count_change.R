# This file takes the gisaid metadata returns a tibble with an index of days,
# a count of the number of sequences that were collected in the prior 30 days
# and were submitted to GISAID prior to the index date, the same metric but 
# lagged the days, and the ratio of these two,

#getwd()

# Libraries ---------------------------------------------------------------
install.packages("tidyverse", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("lubridate", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("zoo", dependencies=TRUE, repos='http://cran.us.r-project.org')

library(tidyverse)
library(lubridate)
library(zoo)

# Load data ---------------------------------------------------------------

#local path
#GISAID_METADATA_PATH<-'../data/raw/metadata.csv'
#SEQ_LAST_30_DAYS_MASTER<- '../data/processed/sequences_last_30_days.csv'
#Domino paths
GISAID_METADATA_PATH<-'/mnt/data/raw/metadata/csv'
SEQ_LAST_30_DAYS_MASTER<- '/mnt/data/processed/sequences_last_30_days.csv'


df <- read.csv(GISAID_METADATA_PATH) %>% 
  filter(collection_date > ymd('2020-1-1'))
master<- read.csv(SEQ_LAST_30_DAYS_MASTER)
  

# Set up arrays -----------------------------------------------------------


date_seq = as.character.Date(seq(ymd('2020-1-1'), today(), by = 'day'))

results = vector(mode = 'integer',
                 length = length(date_seq))


# Loop through calculation ------------------------------------------------

for (i in 1:length(date_seq)){
  
  day_iter = ymd(date_seq[i])
  
  results[i] <-  df %>% 
    filter(collection_date >= day_iter - days(29),
           collection_date <= day_iter,
           submission_date <= day_iter) %>% 
    nrow()
  
  
}

# Combine date and result arrays into a tibble ----------------------------

recent_df <- tibble(date = ymd(date_seq),
                      n = results)

recent_df <- recent_df %>% 
  mutate(n_lag_30 = c(rep(NA_integer_, 30), recent_df$n[1:(nrow(recent_df)-30)]),
         r = n / n_lag_30)

# Concatenate to master file 
combined_df<-rbind(master, recent_df)

# Save time series --------------------------------------------------------

# Domino path
write_csv(combined_df, '/mnt/data/processed/sequences_last_30_days.csv')

# local path
# write_csv(combined_df, '../data/processed/sequences_last_30_days.csv')


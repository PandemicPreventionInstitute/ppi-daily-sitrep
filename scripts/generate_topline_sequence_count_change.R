# This file takes the gisaid metadata returns a tibble with an index of days,
# a count of the number of sequences that were collected in the prior 30 days
# and were submitted to GISAID prior to the index date, the same metric but 
# lagged the days, and the ratio of these two,

getwd()

# Libraries ---------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(zoo)

# Load data ---------------------------------------------------------------


df <- read_csv('../data/processed/inital_clean_metadata.csv',
               col_types = 'iTTlcc') %>% 
  filter(collect_date > ymd('2020-1-1'))
  

# Set up arrays -----------------------------------------------------------


date_seq = as.character.Date(seq(ymd('2020-1-1'), today(), by = 'day'))

results = vector(mode = 'integer',
                 length = length(date_seq))


# Loop through calculation ------------------------------------------------

for (i in 1:length(date_seq)){
  
  day_iter = ymd(date_seq[i])
  
  results[i] <-  df %>% 
    filter(collect_date >= day_iter - days(30),
           collect_date <= day_iter,
           submit_date <= day_iter) %>% 
    nrow()
  
  
}

# Combine date and result arrays into a tibble ----------------------------

combined_df <- tibble(date = ymd(date_seq),
                      n = results)

combined_df <- combined_df %>% 
  mutate(n_lag_30 = c(rep(NA_integer_, 30), combined_df$n[1:(nrow(combined_df)-30)]),
         r = n / n_lag_30)


# Save time series --------------------------------------------------------

write_csv(combined_df, '../data/processed/sequences_last_30_days.csv')


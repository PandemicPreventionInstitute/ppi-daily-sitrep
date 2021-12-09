# Original Author: Nathan Poland
# Updated: Briana Thrift & Zach Susswein
# Current Author: Kaitlyn Johnson

# Date updated: 11-29-2021

# This script takes in the GISAID metadata and OWID and find data and finds the recent cases, tests, and sequences
# It will be used to put the Omicron sequencing data in context


#install.packages("janitor")
#install.packages("countrycode")
#install.packages("lubridate")
#install.packages("tsoutliers")
 
library(tidyverse) # data wrangling
library(tibble) # data wrangling
library(janitor) # column naming
library(countrycode) # country codes
library(lubridate) # date times
library(readxl) # excel import
library(zoo) # calculate rolling averages
library(R.utils) # R utilities
library(stringr) # to parse strings in R
library(tsoutliers) # remove outliers
library(dplyr) # data wrangling
 
# ------ Name data paths and set parameters -------------------------------------------
rm(list = ls())
today <- substr(lubridate::now('EST'), 1, 13)
today <- chartr(old = ' ', new = '-', today)




## Set local file path names
ALL_DATA_PATH<- url("https://raw.githubusercontent.com/dsbbfinddx/FINDCov19TrackerData/master/processed/data_all.csv")
GISAID_DAILY_PATH<-'../data/processed/gisaid_cleaning_output.csv' # this is the file that comes from Briana's processing file
OMICRON_DAILY_CASES<-paste0('../data/processed/metadata_summarized.csv')
BNO_CASES_BY_COUNTRY_PATH<-paste0('../data/raw/daily_BNO_file/', today,'.csv')
BNO_CASES_BY_COUNTRY_DATE<-'../data/raw/BNO_scraped_master.csv'

LAST_DATA_PULL_DATE<-as.Date("2021-11-29") #substr(lubridate::now('EST'), 1, 10) # enter here "YYYY-10-18"
TIME_WINDOW <- 90
TIME_WINDOW_WEEK<- 6 
TIME_WINDOW_MAX_PREV<-30
CONF_LEVEL<-0.95 # confidence we want to be that a variant is not greater than our estimated prevalence
# that has been detected at least once (i.e. what variant prevalence would we be able to detect?)

# adjusted script so that _clean refers to data sets that are in the format country and the metric (i.e. do not contain time series)
# intermediates with time series are designated with _t




# ------ FIND testing data to estimate testing metric -------------------------------------
 
# import csv from FIND containing country, date, population size, tests, 
find_raw <- read.csv(ALL_DATA_PATH) %>%
# standardize names with this janitor function
  clean_names()
 
# select and rename necessary columns, selecting only those related to testing
find_testing_t <- find_raw %>%
  # filter for country set
  filter(set == "country") %>%
  # select time, code, new_tests_corrected, pop_100k, cap_new_tests, all_new_tests, all_cum_cases
  select(name, time, unit, pop_100k, cap_new_tests, cap_cum_tests, all_new_tests, all_cum_tests, all_cum_cases) %>%
  # rename columns as date, code, pop_100k, new_tests_cap, new_tests_all
  rename(country = name, date = time, code= unit, pop_100k = pop_100k, new_tests_cap = cap_new_tests, new_tests_all = all_new_tests, 
          cap_cum_tests = cap_cum_tests, all_cum_tests = all_cum_tests, all_cum_cases = all_cum_cases) %>%
  # parse date as date class
  mutate(date = as.Date(date, format = "%Y-%m-%d"))

#cap cum tests is number of cumulative tests per 100k 
 
# create country code column
find_testing_t <- find_testing_t %>%
  mutate(code = countrycode(country, origin = 'country.name', destination = 'iso3c'))
 
# INSERT A TEST TO PULL OUT THE COUNTRIES MISSING CODES???

# inserts missing country codes manually -- HOW DID WE KNOW THESE WERE MISSING?
find_testing_t$code[find_testing_t$country == "Kosovo"] <- "XKX"
find_testing_t$code[find_testing_t$country == "Namibia"] <- "NAM"

# CHECK THAT DATA SET HAS COMPLETED DATE TIME SERIES
# set start date
first_date<-min(find_testing_t$date, na.rm = TRUE)
date <- seq.Date(first_date, LAST_DATA_PULL_DATE, by = "day")
code <-unique(find_testing_t$code)
date_country<-expand_grid(date, code)
find_testing_t<-left_join(date_country,find_testing_t, by = c("code", "date"))

# Fill in the NAs on the values 
find_testing_t$new_tests_cap[is.na(find_testing_t$new_tests_cap)]<-0
find_testing_t$new_tests_all[is.na(find_testing_t$new_tests_all)]<-0



# create variable for 30 day rolling average of new tests per capita
find_testing_t <- find_testing_t %>%
  # group rows by country code
  group_by(code) %>%
  # create column for 30 day rolling average
  mutate(
    new_tests_cap_avg = round(zoo::rollmean(new_tests_cap, 30, fill = NA),2)
  )
 
 
# display max 30 day rolling average of new tests per capita for each country
# removes time element so now just country and metrics
find_testing_clean <- find_testing_t %>%
  group_by(code) %>%
  summarise(
    max_new_tests_cap_avg = max(new_tests_cap_avg, na.rm = T),
    cap_cum_tests = max(cap_cum_tests, na.rm = T),
    cum_tpr = max(all_cum_cases, na.rm = T)/max(all_cum_tests, na.rm = T)
  )


# Add in recent testing capacity metrics ie average test positivity over past 90 days
# Subset to last 7 days of data

tests_recent<-find_testing_t%>%filter(date>=(LAST_DATA_PULL_DATE - TIME_WINDOW_WEEK) & 
                                          date<=LAST_DATA_PULL_DATE)%>%
  group_by(code)%>%
  summarise(tests_in_last_7_days = sum(new_tests_all))

tests_30_days<-find_testing_t%>%filter(date>=(LAST_DATA_PULL_DATE - 30) & 
                                        date<=LAST_DATA_PULL_DATE)%>%
  group_by(code)%>%
  summarise(tests_in_last_30_days = sum(new_tests_all))

tests_recent<-left_join(tests_recent, tests_30_days, by = "code")

find_testing_clean<-left_join(find_testing_clean, tests_recent, by = "code")

 
# remove any -Inf
find_testing_clean$max_new_tests_cap_avg <- ifelse(find_testing_clean$max_new_tests_cap_avg < 0, NA, find_testing_clean$max_new_tests_cap_avg)
 
# remove any Inf
find_testing_clean$cum_tpr <- ifelse(find_testing_clean$cum_tpr < 0 | find_testing_clean$cum_tpr > 1, NA, find_testing_clean$cum_tpr)
 

 

# -------- GISAID data on SARS-CoV-2 sequences by country and time --------------------------------

gisaid_raw <- read.csv(GISAID_DAILY_PATH) %>% # raw refers to all variants, by country, by day
  # standardize names with this janitor function
  clean_names()

# generate country codes from GISAID country names
gisaid_raw$country_code <- countrycode(gisaid_raw$gisaid_country, origin = 'country.name', destination = 'iso3c')

# Remove any rows that don't contain country codes (i.e. not a valid country)
gisaid_raw<-gisaid_raw[!is.na(gisaid_raw$country_code),]

# generate country codes from OWID country names
gisaid_raw$country_code_owid <- countrycode(gisaid_raw$owid_location, origin = 'country.name', destination = 'iso3c')

# inserts missing country codes
gisaid_raw$country_code[gisaid_raw$country == "Micronesia (country)"] <- "FSM"
gisaid_raw$country_code[gisaid_raw$country == "Timor"] <- "TLS"
gisaid_raw$country_code[gisaid_raw$country == "Turks and Caicos Islands"] <- "TCA"
gisaid_raw$country_code[gisaid_raw$country == "Nauru"] <- "NRU"
gisaid_raw$country_code[gisaid_raw$country == "Kosovo"] <- "XKX"
gisaid_raw$country_code[gisaid_raw$country == "Guernsey"] <- "GGY"
gisaid_raw$country_code[gisaid_raw$country == "Falkland Islands"] <- "FLK"

# parse collection dates as dates
# any observations with only year or year-month become NA
gisaid_raw$collection_date <- as.Date(as.character(gisaid_raw$gisaid_collect_date), format = "%Y-%m-%d")

# parse submission dates as dates
#print("FLAG: THIS IS WHAT WILL BE DELETED WHEN WE GET SUBMISSION DATE")
#gisaid_raw$submission_date <- as.Date(as.character(gisaid_raw$owid_date), format = "%Y-%m-%d")

gisaid_t <- gisaid_raw%>%select(collection_date, gisaid_country, all_lineages,b_1_1_529,
                                         owid_new_cases, owid_population, country_code, owid_location)%>%
  rename(n_new_sequences = all_lineages)

# CHECK THAT DATA SET HAS COMPLETED DATE TIME SERIES
collection_date <- seq.Date(first_date, LAST_DATA_PULL_DATE, by = "day")
country_code <-unique(gisaid_t$country_code)
date_country<-expand_grid(collection_date, country_code)
gisaid_t<-left_join(date_country,gisaid_t, by = c("country_code", "collection_date"))

# Fill in the NAs on the values 
gisaid_t$n_new_sequences[is.na(gisaid_t$n_new_sequences)]<-0
gisaid_t$owid_new_cases[is.na(gisaid_t$owid_new_cases)]<-0

  
# find 7 day average of new sequences
gisaid_t <- gisaid_t %>%
  # group rows by country code
  group_by(country_code) %>%
  # create column for 7 day rolling average
  mutate(
    seq_7davg = round(zoo::rollmean(n_new_sequences, 7, fill = NA),2),
    gisaid_md_seq_omicron_7davg = round(zoo::rollmean(b_1_1_529, 7, fill = NA), 2),
    pct_omicron_7davg = gisaid_md_seq_omicron_7davg/seq_7davg
  )

# filter to last 60 days 
gisaid_t <- gisaid_t %>%filter(collection_date>=(LAST_DATA_PULL_DATE -60) & 
                                 collection_date<= LAST_DATA_PULL_DATE)%>%
  # group rows by country code
  group_by(country_code) %>%
  # create column for 7 day rolling average
  mutate(
    seq_7davg = round(zoo::rollmean(n_new_sequences, 7, fill = NA),2),
    gisaid_md_seq_omicron_7davg = round(zoo::rollmean(b_1_1_529, 7, fill = NA), 2),
    pct_omicron_7davg = gisaid_md_seq_omicron_7davg/seq_7davg
  )

write.csv(gisaid_t, "../data/gisaid_t.csv")



# Subset to only recent data to get recent sequences and cases by country
gisaid_recent_data<-gisaid_t%>%filter(collection_date>=(LAST_DATA_PULL_DATE -30) & 
                                        collection_date<= LAST_DATA_PULL_DATE)%>%
  group_by(country_code)%>%
  summarise(cases_in_last_30_days = sum(owid_new_cases, na.rm = TRUE),
            sequences_in_last_30_days = sum(n_new_sequences, na.rm = TRUE),
            population_size = max(owid_population, na.rm = TRUE))

# replaces NAs with 0
gisaid_recent_data$sequences_in_last_30_days[is.na(gisaid_recent_data$sequences_in_last_30_days)]<-0
gisaid_recent_data<-gisaid_recent_data%>%
  mutate(percent_of_cases_sequenced_last_30_days = 100*sequences_in_last_30_days/cases_in_last_30_days,
         per_capita_seq_rate_in_last_30_days = 100000*sequences_in_last_30_days/population_size,
         max_prevalence_variant_pct = 100*(1-((1-CONF_LEVEL)^(1/sequences_in_last_30_days))))



# Subset to last 7 days of data
cases_in_last_7_days<-gisaid_t%>%filter(collection_date>=(LAST_DATA_PULL_DATE - TIME_WINDOW_WEEK) & 
                                          collection_date<=LAST_DATA_PULL_DATE)%>%
  group_by(country_code)%>%
  summarise(cases_per_100k_last_7_days = 100000*sum(owid_new_cases)/max(owid_population, na.rm = TRUE))

# join with 30 day summary 
gisaid_recent_data<-left_join(gisaid_recent_data, cases_in_last_7_days, by = "country_code")

# Subset to last 30 days of data
cases_in_last_30_days<-gisaid_t%>%filter(collection_date>=(LAST_DATA_PULL_DATE - 30) & 
                                          collection_date<=LAST_DATA_PULL_DATE)%>%
  group_by(country_code)%>%
  summarise(cases_per_100k_last_30_days = 100000*sum(owid_new_cases)/max(owid_population, na.rm = TRUE))

# join with 30 day summary 
gisaid_recent_data<-left_join(gisaid_recent_data, cases_in_last_30_days, by = "country_code")

gisaid_recent_data<-left_join(gisaid_recent_data, find_testing_clean, by = c("country_code"="code"))

gisaid_recent_data<-gisaid_recent_data%>%
  mutate(tests_per_100k_in_last_7_days= 100000*tests_in_last_7_days/population_size,
         tests_per_100k_in_last_30_days = 100000*tests_in_last_30_days/population_size,
         positivity_in_last_7_days = cases_per_100k_last_7_days/tests_per_100k_in_last_7_days,
         positivity_in_last_30_days = cases_per_100k_last_30_days/tests_per_100k_in_last_30_days)


#---- Merge Omicron cases from GISAID sequenced genomes and BNO table
BNO_omicron<-read.csv(BNO_CASES_BY_COUNTRY_PATH)
BNO_omicron<-BNO_omicron%>%
  rename(BNO_confirmed = confirmed, BNO_probable = probable)%>%
  select(code, BNO_confirmed, BNO_probable)

omicron_t<-read.csv(OMICRON_DAILY_CASES)
omicron_seq<-omicron_t%>%group_by(code)%>%
  summarise(cum_omicron_seq = sum(n, na.rm = TRUE))
omicron_seq<-omicron_seq%>%select(code, cum_omicron_seq)

# combine the two tables
omicron_seq<-full_join(omicron_seq, BNO_omicron, by = "code")
omicron_seq<-distinct(omicron_seq) # remove duplicate rows
omicron_seq$max_omicron<- rep(0, nrow(omicron_seq))
# Set max
for (i in 1:nrow(omicron_seq)){
  omicron_seq$max_omicron[i]<-max(c(omicron_seq$cum_omicron_seq[i], omicron_seq$BNO_confirmed[i]), na.rm = TRUE)
}

omicron_seq$max_omicron[omicron_seq$max_omicron== -Inf]<-NA

omicron_seq<- omicron_seq%>%
  mutate(country_name = countrycode(code, origin = 'iso3c', destination = 'country.name'))


# join GISAID data with omicron sequence counts
gisaid_summary_df<-left_join(gisaid_recent_data, omicron_seq, by = c("country_code" = "code"))

# Make a column with omicron sequences where absenses are NAs
gisaid_summary_df$cum_omicron_seq_NA<-gisaid_summary_df$cum_omicron_seq
gisaid_summary_df$max_omicron_seq_NA<-gisaid_summary_df$max_omicron

#Make another column where missing omicron sequences are 0s
gisaid_summary_df$cum_omicron_seq[is.na(gisaid_summary_df$cum_omicron_seq)]<-0
gisaid_summary_df$max_omicron[is.na(gisaid_summary_df$max_omicron)]<-0

# Log transform max_prevalence_variant_pct
gisaid_summary_df$log10_max_prevalence_variant_pct<-log10(gisaid_summary_df$max_prevalence_variant_pct)

# Round percent to 2 decimal points
gisaid_summary_df$max_prevalence_variant_pct[gisaid_summary_df$max_prevalence_variant_pct>0.01]<-round(
      gisaid_summary_df$max_prevalence_variant_pct[gisaid_summary_df$max_prevalence_variant_pct>0.01],2)
# If <0.01, set as 0.01
gisaid_summary_df$max_prevalence_variant_pct[gisaid_summary_df$max_prevalence_variant_pct<=0.01]<-0.01


# Add a column with numbers and flags
gisaid_summary_df$max_prevalence_variant_pct_flags<-gisaid_summary_df$max_prevalence_variant_pct
gisaid_summary_df$max_prevalence_variant_pct_flags[gisaid_summary_df$max_prevalence_variant_pct<=0.01]<-'<0.01'
gisaid_summary_df$max_prevalence_variant_pct_flags[gisaid_summary_df$max_prevalence_variant_pct>=95]<-'not estimated, insufficient recent sequencing'
gisaid_summary_df$max_prevalence_variant_pct_flags[gisaid_summary_df$cum_tpr<0.002 & gisaid_summary_df$max_prevalence_variant_pct>=95]<-'minimal recent COVID cases'

gisaid_summary_df$max_prevalence_variant_pct_w_pct<-gisaid_summary_df$max_prevalence_variant_pct
gisaid_summary_df$max_prevalence_variant_pct_w_pct<-paste0(gisaid_summary_df$max_prevalence_variant_pct, '%')
gisaid_summary_df$max_prevalence_variant_pct_w_pct[gisaid_summary_df$max_prevalence_variant_pct<=0.01]<-'<0.01'
gisaid_summary_df$max_prevalence_variant_pct_w_pct[gisaid_summary_df$max_prevalence_variant_pct>=95]<-'not estimated, insufficient recent sequencing'
gisaid_summary_df$max_prevalence_variant_pct_w_pct[gisaid_summary_df$cum_tpr<0.002 & gisaid_summary_df$max_prevalence_variant_pct>=95]<-'minimal recent COVID cases'


# Write country-level data to csvs
write.csv(omicron_seq, "../data/processed/omicron_seq.csv")
write.csv(gisaid_summary_df, "../data/processed/gisaid_summary_df.csv")

#Country-date level data 
BNO_omicron_t<-read.csv(BNO_CASES_BY_COUNTRY_DATE)
BNO_omicron_t<-unique(BNO_omicron_t)
BNO_omicron_t<-BNO_omicron_t%>%drop_na(confirmed)
BNO_omicron_t<-BNO_omicron_t%>%rename(BNO_confirmed = confirmed, BNO_probable = probable)%>%
  select(code, BNO_confirmed, BNO_probable, timestamp)
BNO_omicron_t$date<-as.Date(substr(BNO_omicron_t$timestamp, 1, 10))

# Keeps only the latest timestamp only for that day
BNO_omicron_t<-BNO_omicron_t%>%group_by(code, date)%>%
  filter(timestamp == max(timestamp))

# Daily summary of BNO for all countries
BNO_omicron_summary<-BNO_omicron_t%>%group_by(date)%>%
  summarise(n_countries_BNO = n(),
            n_case_BNO = sum(BNO_confirmed, na.rm = TRUE))
write.csv(BNO_omicron_summary, "../data/processed/BNO_summary_by_day.csv")

# Do the same with the GISAID omicron sequences
omicron_t<-read.csv(OMICRON_DAILY_CASES)
omicron_t<-omicron_t%>%rename(GISAID_sequences = n)
omicron_t$submission_date<-as.Date(omicron_t$submission_date)
omicron_t$GISAID_sequences<-replace_na(omicron_t$GISAID_sequences, 0)


# Need to make a column for cumulative sequences 
omicron_t<-omicron_t%>%group_by(code)%>%
  mutate(cum_GISAID_seq = cumsum(GISAID_sequences))

# remove rows where cumulative GISAID sequences is 0
omicron_t<-omicron_t%>%filter(cum_GISAID_seq!=0)
  

#Summarise by date (need to make this submission date)
GISAID_omicron_summary<-omicron_t%>%group_by(submission_date)%>%
  summarise(n_countries_GISAID = n(),
            n_seq_GISAID = sum(replace_na(cum_GISAID_seq, 0)))
write.csv(GISAID_omicron_summary, "../data/processed/GISAID_omicron_summary_by_day.csv")

omicron_merge_country_date<-full_join(omicron_t, BNO_omicron_t, by = c("submission_date"= 
"date", "code"= "code"))

omicron_merge_country_date<-distinct(omicron_merge_country_date) # remove duplicate rows
omicron_merge_country_date$max_omicron<- rep(0, nrow(omicron_merge_country_date))
# Set max
for (i in 1:nrow(omicron_merge_country_date)){
  omicron_merge_country_date$max_omicron[i]<-max(c(omicron_merge_country_date$cum_GISAID_seq[i], omicron_merge_country_date$BNO_confirmed[i]), na.rm = TRUE)
}


# On each day, find total countries reporting omicron cases 
# and total cases from max of both sources
omicron_merged_by_day<-omicron_merge_country_date%>%group_by(submission_date)%>%
  summarise(n_countries_all = n(),
            n_cases_all = sum(max_omicron),
            n_seq_GISAID = sum(cum_GISAID_seq, na.rm = TRUE))

# Compute difference from yesterday
today <- as.Date(substr(lubridate::now('EST'), 1, 10))
omicron_daily_change<-omicron_merged_by_day%>%filter(submission_date>=today-1,
                                                     submission_date<=today)%>%
        mutate(daily_inc_countries = diff(n_countries_all),
               daily_inc_GISAID_seq = diff(n_seq_GISAID),
               daily_inc_all_cases = diff(n_cases_all))%>%
          filter(submission_date == today)
write.csv(omicron_daily_change, "../data/processed/omicron_daily_topline.csv")



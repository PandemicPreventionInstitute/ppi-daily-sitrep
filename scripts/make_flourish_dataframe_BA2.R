# Original Author: Nathan Poland
# Updated: Briana Thrift & Zach Susswein
# Current Author: Kaitlyn Johnson

# Date updated: 11-29-2021

# This script takes in the GISAID metadata and OWID and find data and finds the recent cases, tests, and sequences
# It will be used to put the Omicron sequencing data in context


#rm(list = ls())
USE_CASE = Sys.getenv("USE_CASE")
if(USE_CASE == ""){
  USE_CASE<-'local'
}
#USE_CASE = 'domino' # options: 'local' or 'domino'



#---- Libraries----------
if (USE_CASE == 'domino'){
install.packages("tidyverse", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("janitor", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("tibble", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("countrycode", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("lubridate", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("readxl", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("zoo", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("R.utils", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("stringr", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("dplyr", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("scales", dependencies=TRUE, repos='http://cran.us.r-project.org')
}


library(tidyverse) # data wrangling
library(tibble) # data wrangling
library(janitor) # column naming
library(countrycode) # country codes
library(lubridate) # date times
library(readxl) # excel import
library(zoo) # calculate rolling averages
library(R.utils) # R utilities
library(stringr) # to parse strings in R
library(dplyr) # data wrangling
library(scales) # comma formatting
 
# ------ Name data paths and set parameters -------------------------------------------
today <- substr(lubridate::now('EST'), 1, 13)
today <- chartr(old = ' ', new = '-', today)
today_date<-lubridate::today('EST')
#today<-"2021-12-22-13"

## Set filepaths
ALL_DATA_PATH<- url("https://raw.githubusercontent.com/dsbbfinddx/FINDCov19TrackerData/master/processed/data_all.csv")

if (USE_CASE == 'domino'){
GISAID_DAILY_PATH<-'/mnt/data/processed/gisaid_owid_merged.csv' # output from gisaid_metadata_processing.R
SHAPEFILES_FOR_FLOURISH_PATH <- '/mnt/data/static/geometric_country_code_name_master_file.txt'
LAT_LONG_FOR_FLOURISH_PATH<-'/mnt/data/static/country_lat_long_names.csv'
}

if (USE_CASE == 'local'){
ALL_DATA_PATH<- url("https://raw.githubusercontent.com/dsbbfinddx/FINDCov19TrackerData/master/processed/data_all.csv")
GISAID_DAILY_PATH<-'../data/processed/gisaid_owid_merged.csv' # output from gisaid_metadata_processing.R
SHAPEFILES_FOR_FLOURISH_PATH <- '../data/static/geometric_country_code_name_master_file.txt'
LAT_LONG_FOR_FLOURISH_PATH<-'../data/static/country_lat_long_names.csv'
}

if (USE_CASE == 'databricks'){
    GISAID_DAILY_PATH<-"/dbfs/FileStore/tables/ppi-daily-sitrep/data/processed/gisaid_owid_merged.csv" # from extracted datastream
    SHAPEFILES_FOR_FLOURISH_PATH <- '/dbfs/FileStore/tables/ppi-daily-sitrep/data/static/geometric_country_code_name_master_file.txt'
    LAT_LONG_FOR_FLOURISH_PATH<-'/dbfs/FileStore/tables/ppi-daily-sitrep/data/static/country_lat_long_names.csv'
    
}

LAST_DATA_PULL_DATE<-as.Date(substr(lubridate::now('EST'), 1, 10))-days(1) # Make this based off of yesterday!
FIRST_DATE<-"2019-12-01"
TIME_WINDOW <- 29 # since we will include the reference data
TIME_WINDOW_WEEK<- 6 # since will include the reference date
TIME_SERIES_WINDOW<- 89 # last 90 days?
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
    new_tests_cap_avg = round(zoo::rollmean(new_tests_all/pop_100k, 30, fill = NA),2)
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

tests_30_days<-find_testing_t%>%filter(date>=(LAST_DATA_PULL_DATE - TIME_WINDOW) & 
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
print('GISAID metadata successfuly loaded')

# parse collection dates as dates (note this uses the imputed day 15 from metadata processing script)
gisaid_raw$collection_date <- as.Date(as.character(gisaid_raw$gisaid_collect_date), format = "%Y-%m-%d")


gisaid_t <- gisaid_raw%>%select(collection_date, gisaid_country, n_new_sequences, ba_1, ba_2, 
                                         owid_new_cases, owid_population, country_code, owid_location)
gisaid_t$ba_1[is.na(gisaid_t$ba_1)]<-0
gisaid_t$ba_2[is.na(gisaid_t$ba_2)]<-0
  
# find 7 day average of new sequences
gisaid_t <- gisaid_t %>%
  # group rows by country code
  group_by(country_code) %>%
  # create column for 7 day rolling average
  mutate(
    seq_7davg = round(zoo::rollmean(n_new_sequences, 7, fill = NA),2),
    #gisaid_md_seq_omicron_7davg = round(zoo::rollmean(b_1_1_529, 7, fill = NA), 2),# remove because we don't have omicron feed anymore
    #pct_omicron_7davg = gisaid_md_seq_omicron_7davg/seq_7davg, 
    rolling_cases_last_30_days = rollapplyr(owid_new_cases,30,sum, partial = TRUE, align = "right"),
    rolling_cases_last_7_days = rollapplyr(owid_new_cases, 7, sum, partial = TRUE, align = "right"),
    rolling_seq_last_30_days = rollapplyr(n_new_sequences, 30, sum, partial = TRUE, align = "right"),
    percent_of_cases_seq_last_30_days = 100*rolling_seq_last_30_days/rolling_cases_last_30_days,
    rolling_ba2_last_7_days = rollapplyr(ba_2, 7, sum, partial = TRUE, align = "right"),
    rolling_seq_last_7_days = rollapplyr(n_new_sequences, 7, sum, partial = TRUE, align = "right"),
    rolling_pct_of_seq_BA2 = round(100*rolling_ba2_last_7_days/rolling_seq_last_7_days,2)
  )


# filter to last 60 days 
#gisaid_t <- gisaid_t %>%filter(collection_date>=(LAST_DATA_PULL_DATE -TIME_SERIES_WINDOW) & 
                                 #collection_date<= LAST_DATA_PULL_DATE)
# Make sure that the most recent date is yesterday
if(USE_CASE != 'databricks'){
    stopifnot("GISAID metadata run isnt up to date" = max(gisaid_t$collection_date[gisaid_t$n_new_sequences>0]) >= (today_date - days(10)))
#write.csv(gisaid_t, "../data/gisaid_t.csv")
}
print(paste0('Last collection date is ',max(gisaid_t$collection_date[gisaid_t$n_new_sequences>0])))

# Subset to only recent data to get recent sequences and cases by country
gisaid_recent_data<-gisaid_t%>%filter(collection_date>=(LAST_DATA_PULL_DATE -TIME_WINDOW) & 
                                        collection_date<= LAST_DATA_PULL_DATE)%>%
  group_by(country_code)%>%
  summarise(cases_in_last_30_days = sum(owid_new_cases, na.rm = TRUE),
            sequences_in_last_30_days = sum(n_new_sequences, na.rm = TRUE),
            population_size = max(owid_population, na.rm = TRUE),
            cases_per_100k_last_30_days = 100000*sum(owid_new_cases, na.rm = TRUE)/max(owid_population, na.rm = TRUE))

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
  summarise(cases_per_100k_last_7_days = round(100000*sum(owid_new_cases)/max(owid_population, na.rm = TRUE), 1),
            pct_cases_BA2_past_7_days = round(100*sum(ba_2, na.rm = T)/sum(n_new_sequences, na.rm = T), 2))

# join with 30 day summary 
gisaid_recent_data<-left_join(gisaid_recent_data, cases_in_last_7_days, by = "country_code")


# --- Find number of BA.2 cases submitted by country ---- 
gisaid_BA2<-gisaid_t%>%
  group_by(country_code)%>%
  summarise(n_ba_2 = sum(ba_2, na.rm = TRUE),
            n_sequences = sum(n_new_sequences, na.rm = TRUE))

gisaid_recent_data<-left_join(gisaid_recent_data, gisaid_BA2, by = "country_code")

gisaid_recent_data<-left_join(gisaid_recent_data, find_testing_clean, by = c("country_code"="code"))


gisaid_recent_data<-gisaid_recent_data%>%
  mutate(tests_per_100k_in_last_7_days= 100000*tests_in_last_7_days/population_size,
         tests_per_100k_in_last_30_days = 100000*tests_in_last_30_days/population_size,
         positivity_in_last_7_days = cases_per_100k_last_7_days/tests_per_100k_in_last_7_days,
         positivity_in_last_30_days = cases_per_100k_last_30_days/tests_per_100k_in_last_30_days,
        pct_cases_BA2_past_30_days = round(100*n_ba_2/n_sequences,2))

gisaid_BA2_past_week<-gisaid_t%>%filter(collection_date == (LAST_DATA_PULL_DATE - 7))%>%group_by(country_code)%>%
  summarise(pct_seq_BA_2_last_week = max(rolling_pct_of_seq_BA2),
            total_seq_last_week = max(rolling_seq_last_7_days))

# If we wanted to compare to the week before
# gisaid_2_weeks_ago<-gisaid_t%>%filter(collection_date == (LAST_DATA_PULL_DATE - 14))%>%group_by(country_code)%>%
#   summarise(pct_seq_BA_2_two_weeks_ago = max(rolling_pct_of_seq_BA2),
#             total_seq_two_weeks_ago = max(rolling_seq_last_7_days))
# gisaid_BA2_pct<-left_join(gisaid_past_week, gisaid_2_weeks_ago, by = "country_code")
# gisaid_BA2_pct<-gisaid_BA2_pct%>%mutate(change_since_last_week = pct_seq_BA_2_last_week - pct_seq_BA_2_two_weeks_ago)

gisaid_recent_data<-left_join(gisaid_recent_data, gisaid_BA2_past_week, by = "country_code")

gisaid_summary_df<-gisaid_recent_data



# Make a column with omicron sequences where absenses are NAs
gisaid_summary_df$n_ba_2[is.na(gisaid_summary_df$n_ba_2)]<-0 # set countries without ba_2 as 0 
gisaid_summary_df$n_ba_2_NA<-gisaid_summary_df$n_ba_2 # copy over col with 0s
gisaid_summary_df$n_ba_2_NA[gisaid_summary_df$n_ba_2 ==0]<-NA # replace 0s with NA
gisaid_summary_df<-gisaid_summary_df%>%
  mutate(ba_2_detected = case_when(
    n_ba_2 >0 ~ 'Yes',
    n_ba_2 == 0 ~'No',
    is.na(n_ba_2) ~'No'
  ))

# Make a column for the dots
gisaid_summary_df$pct_BA2_dots<-gisaid_summary_df$pct_seq_BA_2_last_week
gisaid_summary_df$pct_BA2_dots[gisaid_summary_df$ba_2_detected == "No"]<-NA
gisaid_summary_df$pct_BA2_dots[(gisaid_summary_df$pct_seq_BA_2_last_week == 0 | is.na(gisaid_summary_df$pct_seq_BA_2_last_week)) & gisaid_summary_df$ba_2_detected == "Yes"]<-0

# Make a column for the pop-up
gisaid_summary_df$pct_BA2_panel<-gisaid_summary_df$pct_seq_BA_2_last_week
gisaid_summary_df$pct_BA2_panel[gisaid_summary_df$total_seq_last_week<100]<-paste0(gisaid_summary_df$pct_seq_BA_2_last_week[gisaid_summary_df$total_seq_last_week<100], ' %*')
gisaid_summary_df$pct_BA2_panel[gisaid_summary_df$total_seq_last_week>100]<-paste0(gisaid_summary_df$pct_seq_BA_2_last_week[gisaid_summary_df$total_seq_last_week>100], ' %')
gisaid_summary_df$pct_BA2_panel[gisaid_summary_df$total_seq_last_week==0]<-'No sequences were submitted from last week'

print('GISAID summary dataframe successful')

# Load and join shapefile for flourish
shapefile <- read_delim(SHAPEFILES_FOR_FLOURISH_PATH, delim = "\t") %>%
  rename(country_code = `3-letter ISO code`) %>%
  select(geometry, Name, country_code)

lat_long<-read.csv(LAT_LONG_FOR_FLOURISH_PATH)%>% clean_names()%>%
  rename(country_code= x3_letter_iso_code) %>%
  select(country_code, latitude, longitude)

print('Shapefiles successfully loaded')

gisaid_summary_df <-left_join(shapefile, gisaid_summary_df, by = 'country_code')
gisaid_summary_df <- left_join(lat_long, gisaid_summary_df, by = "country_code")


# Make a column with the % of sequences that are BA.2 for the 

gisaid_summary_df<-distinct(gisaid_summary_df)

US_df<-gisaid_summary_df[gisaid_summary_df$country_code == "USA",]

stopifnot('USA has less than 50 cases per 100k in last 7 days' = US_df$cases_per_100k_last_7_days>50)
stopifnot('USA has no sequencing data' = US_df$percent_of_cases_sequenced_last_30_days >0.0001)
stopifnot('USA BA2 data not being detected' = US_df$n_ba_2>40)
stopifnot('Country reporting greater than 100% BA.2' = sum(gisaid_summary_df$pct_BA2_dots>100, na.rm = T)==0)
stopifnot('USA reporting less than 50% BA.2' = US_df$pct_BA2_dots>50)
print(paste0('US is reporting ', US_df$pct_BA2_dots, '% BA.2 and its subvariants'))

# select cols for flourish and ensure that they're present in the df
stopifnot ("Error: gisaid_summary_df.csv does not contain all necessary columns" = 
             c('geometry', 'latitude', 'longitude', 'Name', 'n_ba_2',
               'n_ba_2_NA', 'ba_2_detected', 'cases_per_100k_last_7_days', 'pct_BA2_dots', 'pct_BA2_panel') %in% colnames(gisaid_summary_df))

# Check to make sure cases are filled in for say USA
stopifnot("Error: case data not in gisaid_summary_df" = 
            !is.na(gisaid_summary_df$cases_per_100k_last_7_days[gisaid_summary_df$country_code=="USA"]))
print('Passed unit tests')

# only output the necessary columns!
gisaid_summary_df<-gisaid_summary_df%>%mutate(
  rounded_pct_cases_seq = round(percent_of_cases_sequenced_last_30_days, 2))
gisaid_summary_df$rounded_pct_cases_seq[is.na(gisaid_summary_df$rounded_pct_cases_seq)]<-0
gisaid_summary_df$pct_cases_seq_w_NA<-gisaid_summary_df$rounded_pct_cases_seq
gisaid_summary_df$pct_cases_seq_w_NA[gisaid_summary_df$pct_cases_seq_w_NA==0]<-NA

gisaid_summary_df<- gisaid_summary_df %>% select(geometry,latitude, longitude, Name, n_ba_2, 
                                                 n_ba_2_NA, ba_2_detected, cases_per_100k_last_7_days, 
                                                 percent_of_cases_sequenced_last_30_days, rounded_pct_cases_seq,
                                                 pct_cases_seq_w_NA, sequences_in_last_30_days, pct_seq_BA_2_last_week, pct_BA2_dots, pct_BA2_panel, total_seq_last_week)
gisaid_summary_df$sequences_in_last_30_days[is.na(gisaid_summary_df$sequences_in_last_30_days)]<-0


#gisaid_summary_df<-left_join(gisaid_summary_df)
# Ready to for output
gisaid_summary_df<-distinct(gisaid_summary_df)


if (USE_CASE == "domino"){
  write.csv(gisaid_summary_df, "/mnt/data/processed/gisaid_summary_df_ba2.csv")
}

if (USE_CASE == "local"){
  write.csv(gisaid_summary_df, "../data/processed/gisaid_summary_df_ba2.csv")
}


if (USE_CASE == "databricks"){
    write.csv(gisaid_summary_df, "/dbfs/FileStore/tables/ppi-daily-sitrep/data/processed/gisaid_summary_df_ba2_db.csv")
}

print('File saved successfully')



# test

# Make some figures real quick

subset_df<-gisaid_t%>%filter(country_code == "USA" | country_code == "GBR", collection_date> ymd("2022-01-01"))
subset_df%>%ggplot() + geom_line(aes(x = collection_date, y = seq_7davg, color = gisaid_country))
subset_df%>%ggplot() + geom_line(aes(x = collection_date, y = percent_of_cases_seq_last_30_days, color = gisaid_country))
subset_df%>%ggplot() + geom_line(aes(x = collection_date, y = rolling_cases_last_30_days, color = gisaid_country))
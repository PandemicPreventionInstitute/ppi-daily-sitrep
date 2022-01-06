#Kaitlyn Johnson
# process metadata from GISAID automated data stream, combine with owid cases and population and generate
# gisaid_cleaning_output.csv which contains number of sequences by collection date by country and day,
# and number of new cases by country and day
# V1 does not include median and quartiles of lag time calculation

#Jan 3rd 2022
rm(list = ls())

#------Libraries------------
install.packages("tidyverse", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("janitor", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("tibble", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("countrycode", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("lubridate", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("readxl", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("zoo", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("R.utils", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("stringr", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("tsoutliers", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("dplyr", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("scales", dependencies=TRUE, repos='http://cran.us.r-project.org')

library(tidyverse) # data wrangling
library(tibble) # data wrangling
library(janitor) # column naming
library(countrycode) # country c?des
library(lubridate) # date times
library(readxl) # excel import
library(zoo) # calculate rolling averages
library(R.utils) # R utilities
library(stringr) # to parse strings in R
library(tsoutliers) # remove outliers
library(dplyr) # data wrangling

#-----Filepaths------------
#local
#GISAID_METADATA_PATH<-"../data/raw/metadata.csv" # from extracted datastream
#Domino
GISAID_METADATA_PATH<-"/mnt/data/raw/metadata.csv" # from extracted datastream
OWID_PATH<-url('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv')
FUTURE_DATE_PATH<-'/mnt/data/suspect_date.csv'


#-----Download and process------


#1. Download data
metadata<-read.csv(GISAID_METADATA_PATH)
owid_raw<-read_csv(OWID_PATH)



# 2. Separate GISAID metadata location into continent, country, division, and location
metadata<- metadata %>%
    separate(location,
             into = c("continent", "country", "division", "location"),
             sep = " / | /|/ |/")

# 3. Fix random things in location names
metadata$country[metadata$country == "USA"] <- "United States"
# replace Usa acronym with United States
metadata$country[metadata$country == "Usa"] <- "United States"
# replace DC with District of Columbia
metadata$division[metadata$division == "DC"] <- "District of Columbia"
# capitalize first letter of country
metadata$country <- capitalize(metadata$country)
# correct mispelling
metadata$country[metadata$country == "Cote dIvoire"] <- "Cote d'Ivoire"
# correct mispelling
metadata$country[metadata$country == "Niogeria"] <- "Nigeria"
# correct mispelling
metadata$country[metadata$country == "Republic of the Congo"] <- "Congo"
# correct mispelling
metadata$country[metadata$country == "Czech republic"] <- "Czech Republic"
# correct misentry of Lithuania
metadata$country[metadata$country == "Jonavos apskritis"] <- "Lithuania"
# correct misreading
metadata$country[metadata$country == "M?xico"] <- "Mexico"

# 4. Deal with date issues
# Assign submissions with missing date to the 15th of the month
metadata$collection_date <- ifelse(nchar(as.character(metadata$collection_date)) == 7, paste(as.character(metadata$collection_date), "15", sep = "-"), as.character(metadata$collection_date))
# format as dates
metadata$collection_date<-ymd(metadata$collection_date)
# exclude submissions earlier than 2019-12-01
metadata<- metadata[metadata$collection_date >= as.Date("2019-12-01", format = "%Y-%m-%d"),]
# exclude submissions dated to the future
metadata <- metadata[metadata$collection_date <= as.Date(Sys.Date(), format = "%Y-%m-%d"),]
# create masterlist of sequences with collection date in future of when they were submitted

suspect_date <- read_csv(FUTURE_DATE_PATH,
                         col_types = 'c') %>% 
  bind_rows(metadata %>% 
              filter((collection_date > today()) | 
                      (submission_date > today()) |
                       (collection_date > submission_date)) %>% 
              select(accession_id)) %>% 
  unique()



metadata['is_suspect_date'] = metadata['accession_id'] %in% suspect_date['accession_id']

# 5. Exclude sequences in Next Strain exclusion list by assession ID

## WRONG: NEEDS TO USE NAME NOT ACCESSION ID -- NEED TO UPDATE FEED
#ns_exclude <- read_delim('https://raw.githubusercontent.com/nextstrain/ncov/master/defaults/exclude.txt',
#                         delim = '\n',
#                         col_names = 'accession_id',
#                         col_types = 'c') %>% 
#  filter(stringr::str_starts(string = accession_id,
#                              pattern = '#',
#                              negate = TRUE),
#         accession_id != '') %>% 
#  mutate(accession_id = paste0('hCOV-19/', accession_id))


# 6. Generate country codes from GISAID country names
metadata$code <- countrycode(metadata$country, origin = 'country.name', destination = 'iso3c')

# 7. Calculate the percent of cases sequenced in last 30 days and previous 30 days GLOBALLY
# date_seq = as.character.Date(seq(ymd('2020-1-1'), today(), by = 'day'))
# results = vector(mode = 'integer',
#                  length = length(date_seq))
# # Loop through calculation ------------------------------------------------
# for (i in 1:length(date_seq)){
#     day_iter = ymd(date_seq[i])
#     results[i] <-  metadata %>%
#         filter(collection_date >= day_iter - days(29),
#                collection_date <= day_iter,
#                submission_date <= day_iter) %>%
#         nrow()
# }
# # Combine date and result arrays into a tibble ----------------------------
# combined_df <- tibble(date = ymd(date_seq),
#                       n = results)
# combined_df <- combined_df %>%
#     mutate(n_lag_30 = c(rep(NA_integer_, 30), combined_df$n[1:(nrow(combined_df)-30)]),
#            r = n / n_lag_30)



# 8. dataframe for total sequences in GISAID by country day
gisaid_t <- metadata %>%
    group_by(code, country, collection_date) %>%
    summarise(n_new_sequences = n()) %>%
    select(code, country, collection_date, n_new_sequences)%>%
    rename(country_code = code,
           gisaid_collect_date = collection_date,
           gisaid_country = country)

# 9. Process OWID data 
# only keep data after Dec. 2019
owid<- owid_raw[owid_raw$date >= as.Date("2019-12-01", format = "%Y-%m-%d"),]
owid$date<-as.Date(owid$date, format = "%Y-%m-%d")
# Drop OWID region rows that start with OWID_ except specific locations
owid<-owid%>%filter(!iso_code %in% c("OWID_AFR","OWID_ASI", "OWID_EUR", "OWID_EUN",
                                     "OWID_INT", "OWID_NAM", "OWID_OCE",
                                     "OWID_SAM", "OWID_WRL"))
#select column names
owid<-owid%>%select(date,location,iso_code,continent,new_cases,
                    new_cases_smoothed,population,people_vaccinated,
                    people_fully_vaccinated)
# append owid to colnames
colnames(owid) <- paste("owid", colnames(owid),sep="_")

# 10. Merge with GISAID metadata 
merged_df<-full_join(gisaid_t, owid, by = c("gisaid_collect_date"= "owid_date", "country_code"= "owid_iso_code"))



#-------Write data to file---------

#local
#write.csv(merged_df, '../data/processed/gisaid_owid_merged.csv', row.names = FALSE)
#write_csv(combined_df, '../data/processed/sequences_last_30_days.csv')
#Domino
write.csv(merged_df, '/mnt/data/processed/gisaid_owid_merged.csv', row.names = FALSE)
write_csv(suspect_date, '/mnt/data/suspect_date.csv')

#write_csv(combined_df, '/mnt/data/processed/sequences_last_30_days.csv')
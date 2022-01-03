#Kaitlyn Johnson
#process metadata from GISAID automated data stream
#Jan 3rd 2022
rm(list = ls())
###########
#Libraries#
###########
library(tidyverse)
library(lubridate)
library(zoo)

######################
#Download and process#
######################

#1. Download data
metadata<-read_csv("../data/raw/metadata.csv")



# 2. Separate location into continent, country, division, and location
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
# exclude submissions earlier than 2019-12-01
metadata<- metadata[metadata$collection_date >= as.Date("2019-12-01", format = "%Y-%m-%d"),]
# exclude submissions dated to the future
metadata <- metadata[metadata$collection_date <= as.Date(Sys.Date(), format = "%Y-%m-%d"),]
# create masterlist of sequences with collection date in future of when they were submitted

suspect_date <- read_csv('../data/suspect_date.csv',
                         col_types = 'c') %>% 
  bind_rows(metadata %>% 
              filter((collection_date > today()) | 
                      (submission_date > today()) |
                       (collection_date > submission_date)) %>% 
              select(accession_id)) %>% 
  unique()

write_csv(suspect_date, '../data/suspect_date.csv')


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
date_seq = as.character.Date(seq(ymd('2020-1-1'), today(), by = 'day'))
results = vector(mode = 'integer',
                 length = length(date_seq))
# Loop through calculation ------------------------------------------------
for (i in 1:length(date_seq)){
    day_iter = ymd(date_seq[i])
    results[i] <-  metadata %>% 
        filter(collection_date >= day_iter - days(29),
               collection_date <= day_iter,
               submission_date <= day_iter) %>% 
        nrow()
}
# Combine date and result arrays into a tibble ----------------------------
combined_df <- tibble(date = ymd(date_seq),
                      n = results)
combined_df <- combined_df %>% 
    mutate(n_lag_30 = c(rep(NA_integer_, 30), combined_df$n[1:(nrow(combined_df)-30)]),
           r = n / n_lag_30)



# 7. dataframe for total sequences in GISAID by country
gisaid_metadata_clean <- gisaid_metadata_raw %>%
    group_by(code) %>%
    mutate(total_sequences = n()) %>%
    select(code, total_sequences)

# display one row per country
gisaid_metadata_clean <- gisaid_metadata_clean[!duplicated(gisaid_metadata_clean$code),]

# find_clean: merge GISAID metadata into template
find_clean <- left_join(find_clean, gisaid_meta?ata_clean, by = c("code" = "code"))

# replace NA with 0 for total_sequences
find_clean$total_sequences <- ifelse(is.na(find_clean$total_sequences) == T, 0, find_clean$total_sequences)

# import csv from GISAID provision (curl https://najapoland:ofEJOnWEOf?3@www.epicov.org/epi3/3p/finddx/export/provision.json.xz | xz -d -T0 > ./provision.json)
gisaid_provision <- read.csv("provision.csv",
                             na.strings = c("", "?")) %>%
    # standardize names with this janitor function
    clean_names(?
                    
                    # parse dates as dates
                    gisaid_provision$created <- as.Date(as.character(gisaid_provision$created), format = "%Y-%m-%d")
                
                # generate country codes from GISAID country names
                gisaid_provision$code <- countrycode(gisaid_provision$country, origin = 'country.n?me', destination = 'iso3c')
                
                # insert country code for Saint Martin
                gisaid_provision$code[gisaid_provision$country=="Saint Martin"] <- "MAF"
                
                # insert country code for Guyana
                gisaid_provision$code[gisaid_provision$country=="Guyane"] <- "GUY"
                
                # insert coun?ry code for Kosovo
                gisaid_provision$code[gisaid_provision$country=="Kosovo"] <- "XKX"
                
                # select columns
                gisaid_provision <- gisaid_provision %>%
                    select(submission_count, code)
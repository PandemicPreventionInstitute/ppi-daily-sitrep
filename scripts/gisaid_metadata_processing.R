#Kaitlyn Johnson
# process metadata from GISAID automated data stream, combine with owid cases and population and generate
# gisaid_cleaning_output.csv which contains number of sequences by collection date by country and day,
# and number of new cases by country and day
# V1 does not include median and quartiles of lag time calculation

#Jan 3rd 2022
rm(list = ls())

USE_CASE = Sys.getenv("USE_CASE")
if(USE_CASE == ""){
  USE_CASE<-'local'
}

print(USE_CASE) 
FROM_FEED<-FALSE

#------Libraries------------
if (USE_CASE== 'domino'){
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
install.packages("readr", dependencies=TRUE, repos='http://cran.us.r-project.org')
}

library(tidyverse) # data wrangling
library(tibble) # data wrangling
library(janitor) # column naming
library(countrycode) # country c?des
library(lubridate) # date times
library(readxl) # excel import
library(zoo) # calculate rolling averages
library(R.utils) # R utilities
library(stringr) # to parse strings in R
library(dplyr) # data wrangling
library(readr) # read_csv

#-----Filepaths------------
#local
if (USE_CASE == 'local'){
GISAID_METADATA_PATH<-"../data/raw/metadata.csv" # from extracted datastream
GISAID_METADATA_DWNLD_PATH<- "../data/raw/metadata.tsv"
OWID_PATH<-url('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv')
FUTURE_DATE_PATH<-'../data/suspect_date.csv'
}
#Domino
if (USE_CASE == 'domino'){
GISAID_METADATA_PATH<-"/mnt/data/raw/metadata.csv" # from extracted datastream
OWID_PATH<-url('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv')
FUTURE_DATE_PATH<-'/mnt/data/suspect_date.csv'
}

if (USE_CASE == 'databricks'){
    GISAID_METADATA_PATH<-"/dbfs/FileStore/tables/ppi-daily-sitrep/data/raw/metadata.csv" # from extracted datastream
    OWID_PATH<-url('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv')
}


FIRST_DATE<-"2019-12-01" # earliest date we want COVID cases for 
#-----Download and process------


#1. Download data
if (FROM_FEED == TRUE){
    metadata<-read.csv(GISAID_METADATA_PATH)
}
if (FROM_FEED == FALSE){
    metadata<-read.delim(GISAID_METADATA_DWNLD_PATH)%>%clean_names()%>%
        filter(host == "Human", type == "betacoronavirus")
}
owid_raw<-read_csv(OWID_PATH)
print('OWID data loaded successfully')



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

# format as dates
metadata$collection_date<-ymd(metadata$collection_date)
metadata$submission_date<-ymd(metadata$submission_date)
# exclude submissions earlier than 2019-12-01
metadata<- metadata[metadata$collection_date >= as.Date(FIRST_DATE, format = "%Y-%m-%d"),]
# exclude submissions dated to the future
metadata <- metadata[metadata$collection_date <= as.Date(Sys.Date(), format = "%Y-%m-%d"),]
# create masterlist of sequences with collection date in future of when they were submitted

# TEST for update cadence
if (USE_CASE != 'databricks'){ # when troubleshooting for db, want to see if this will run 
    #even if metadata not updated (because auto extraction still not working)
stopifnot(' Metadata is more than 4 days out of date' = max(metadata$submission_date, na.rm = T) >=today() - days(4))
}

print(paste0('Metadata loaded successfully, last submission date is ', max(metadata$submission_date, na.rm = T)))
print(paste0('Metadata loaded successfully, last collection date is ', max(metadata$collection_date, na.rm = T)))
# 5. Exclude sequences in Next Strain exclusion list by assession ID

# 6. Generate country codes from GISAID country names
metadata$code <- countrycode(metadata$country, origin = 'country.name', destination = 'iso3c')

# inserts missing country codes
metadata$code[metadata$country == "Micronesia (country)"] <- "FSM"
metadata$code[metadata$country == "Timor"] <- "TLS"
metadata$code[metadata$country == "Turks and Caicos Islands"] <- "TCA"
metadata$code[metadata$country == "Nauru"] <- "NRU"
metadata$code[metadata$country == "Kosovo"] <- "XKX"
metadata$code[metadata$country == "Guernsey"] <- "GGY"
metadata$code[metadata$country == "Falkland Islands"] <- "FLK"

# Remove any rows that don't contain country codes (i.e. not a valid country)
metadata<-metadata[!is.na(metadata$code),]
# Remove any rows that don't contain valid collection or submission dates
metadata<-metadata[!is.na(metadata$collection_date),]
metadata<-metadata[!is.na(metadata$submission_date),]

# Read in the list of accession ids with dates that are suspect, check for suspect dates, and add
# suspect_date <- read_csv(FUTURE_DATE_PATH,
#                          col_types = 'c') %>% 
#   bind_rows(metadata %>% 
#               filter((collection_date > today()) | 
#                        (submission_date > today()) |
#                        (collection_date > submission_date)) %>% 
#               select(accession_id)) %>% 
#   unique()
# 
# 
# metadata['is_suspect_date'] = metadata['accession_id'] %in% suspect_date['accession_id']

# # 7. Calculate the percent of cases sequenced in last 30 days and previous 30 days GLOBALLY
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
  group_by(code, collection_date) %>%
  summarise(n_new_sequences = n()) %>%
  select(code, collection_date, n_new_sequences)%>%
  rename(country_code = code,
         gisaid_collect_date = collection_date)

# 8. Data frame with total number of ba.1 sequences by country day
gisaid_t_BA_1<-metadata %>%group_by(code, collection_date) %>%
  filter(pango_lineage == "BA.1") %>%
  summarise(ba_1 = n())%>%select(code, collection_date, ba_1)%>%
  rename(country_code = code,
         gisaid_collect_date = collection_date)
gisaid_t<-left_join(gisaid_t, gisaid_t_BA_1, by = c("gisaid_collect_date", "country_code"))

# 9. Data frame with total number of ba.2 sequences by country day
gisaid_t_BA_2<-metadata %>%group_by(code, collection_date) %>%
    filter(grepl("BA.2", pango_lineage, fixed = TRUE)) %>%
  #filter(pango_lineage == "BA.2") %>%
  summarise(ba_2 = n())%>%select(code, collection_date, ba_2)%>%
  rename(country_code = code,
         gisaid_collect_date = collection_date)
gisaid_t<-left_join(gisaid_t, gisaid_t_BA_2, by = c("gisaid_collect_date", "country_code"))

# 9. Data frame with total number of ba.4/ba.5 sequences by country day
gisaid_t_BA_4_5<-metadata %>%group_by(code, collection_date) %>%
    filter(grepl("BA.4", pango_lineage, fixed = TRUE) |
           grepl("BA.5", pango_lineage, fixed = TRUE)) %>%
    summarise(ba_4_5 = n())%>%select(code, collection_date, ba_4_5)%>%
    rename(country_code = code,
           gisaid_collect_date = collection_date)
gisaid_t<-left_join(gisaid_t, gisaid_t_BA_4_5, by = c("gisaid_collect_date", "country_code"))
# Replace NAs with 0s
gisaid_t$ba_1[is.na(gisaid_t$ba_1)]<-0
gisaid_t$ba_2[is.na(gisaid_t$ba_2)]<-0
gisaid_t$ba_4_5[is.na(gisaid_t$ba_4_5)]<-0

# 10. All other sequences not ba_1 & ba_2 (presumably some version of delta)
gisaid_t<-gisaid_t%>%mutate(
  other = n_new_sequences - ba_1 - ba_2 - ba_4_5)





# Add in code to complete through yesterday with 0s for each country 
gisaid_collect_date <- seq.Date(as.Date(FIRST_DATE), today()-1, by = "day")
country_code <-unique(gisaid_t$country_code)
n_gisaid_codes<-length(country_code)
date_country<-expand_grid(gisaid_collect_date, country_code)
gisaid_t<-left_join(date_country,gisaid_t, by = c("country_code", "gisaid_collect_date"))
gisaid_t<-gisaid_t%>%mutate(gisaid_country = 
                              countrycode(country_code, origin = 'iso3c', destination = 'country.name'))


# 9. Process OWID data 
# only keep data after Dec. 2019
owid<- owid_raw[owid_raw$date >= as.Date(FIRST_DATE, format = "%Y-%m-%d"),]
owid$date<-as.Date(owid$date, format = "%Y-%m-%d")
# Drop OWID region rows that start with OWID_ except specific locations
owid<-owid%>%filter(!iso_code %in% c("OWID_AFR","OWID_ASI", "OWID_EUR", "OWID_EUN",
                                     "OWID_INT", "OWID_NAM", "OWID_OCE",
                                     "OWID_SAM", "OWID_WRL",
                                     "OWID_CYN", "OWID_HIC", "OWID_KOS", "OWID_LIC", # not in python processing
                                     "OWID_LMC", "OWID_UMC")) # These ones weren't in python processing but should be?
#select column names
owid<-owid%>%select(date,location,iso_code,continent,new_cases,
                    new_cases_smoothed,population,people_vaccinated,
                    people_fully_vaccinated) # option to add more here!
# Check that number of codes = number of countries
n_owid_codes<-unique(owid$iso_code)
n_owid_countries<-unique(owid$location)
stopifnot('More countries than codes, need to sum countries by code'=
            length(n_owid_codes)==length(n_owid_countries))
stopifnot('GISAID has more data than OWID'= length(n_gisaid_codes)<length(n_owid_codes))

# Fill in the missing country-days (same as with GISAID)
date<-gisaid_collect_date
iso_code <-unique(owid$iso_code)
date_codes_owid<-expand_grid(date, iso_code)
owid<-left_join(date_codes_owid,owid, by = c("iso_code", "date"))
owid<-owid%>%mutate(continent =  countrycode(iso_code, origin = 'iso3c', destination = 'continent'))


# append owid to colnames
colnames(owid) <- paste("owid", colnames(owid),sep="_")




# 10. Merge with GISAID metadata 
merged_df<-left_join(owid, gisaid_t, by = c("owid_date"="gisaid_collect_date",  "owid_iso_code"="country_code" ))
print('OWID and GISAID data successfully merged')

# Fill in the NAs on the values 
merged_df$n_new_sequences[is.na(merged_df$n_new_sequences)]<-0
merged_df$owid_new_cases[is.na(merged_df$owid_new_cases)]<-0
merged_df$owid_new_cases_smoothed[is.na(merged_df$owid_new_cases_smoothed)]<-0

merged_df<-merged_df%>%rename(gisaid_collect_date= owid_date,
                              country_code = owid_iso_code)


n_global_cases<-sum(merged_df$owid_new_cases)



#-------Write data to file---------

#local
if (USE_CASE == 'local'){
write.csv(merged_df, '../data/processed/gisaid_owid_merged.csv', row.names = FALSE)
#write_csv(suspect_date, '../data/suspect_date.csv')
}

if (USE_CASE == 'domino'){
#Domino
write.csv(merged_df, '/mnt/data/processed/gisaid_owid_merged.csv', row.names = FALSE)
#write_csv(suspect_date, '/mnt/data/suspect_date.csv')
#write_csv(combined_df, '/mnt/data/processed/sequences_last_30_days.csv')
}
if (USE_CASE == 'databricks'){
    #Domino
    write.csv(merged_df, '/dbfs/FileStore/tables/ppi-daily-sitrep/data/processed/gisaid_owid_merged.csv', row.names = FALSE)
    #write_csv(suspect_date, '/mnt/data/suspect_date.csv')
    #write_csv(combined_df, '/mnt/data/processed/sequences_last_30_days.csv')
    print('File saved successfully')
}



# Find the number of sequences submitted in the previous week and 30 days over time 
country_list<-c("USA", "GBR","DEU")
date_seq = as.character.Date(seq(ymd("2021-11-01"), today()-1, by = 'day'))
for (j in 1:length(country_list)){
    df<-metadata%>%filter(code == country_list[j])
    code <-country_list[j]

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
    
    combined_df <- tibble(date = ymd(date_seq),
                          n_seq_last_30_d = results)
    if(j==1){
        df_t<-cbind(code,combined_df)
    }
    
    else{
        df_ti<-cbind(code, combined_df)
        df_t<-rbind(df_t, df_ti)
    }
}

df_t<-left_join(df_t, merged_df, by = c("code" = "country_code", "date" = "gisaid_collect_date"))

df_t<-df_t%>%group_by(code)%>%mutate(
    rolling_cases_last_30_days = rollapplyr(owid_new_cases,30,sum, partial = TRUE, align = "right"),
    pct_cases_seq_last_30_days = 100*n_seq_last_30_d/rolling_cases_last_30_days)%>%
    filter(date >= ymd("2021-12-01"))

df_t%>%ggplot() + geom_line(aes(x = date, y = n_seq_last_30_d, color = code)) + 
    xlab('Date') + ylab('Sequences in prior 30 days') + 
    ggtitle('Number of sequences that had been submitted in previous 30 days') +theme_bw()

df_t%>%ggplot() + geom_line(aes(x = date, y = pct_cases_seq_last_30_days, color = code)) + 
    xlab('Date') + ylab('% cases sequenced last 30 days') + 
    ggtitle('% of cases sequenced last 30 days') +theme_bw()

df_t%>%ggplot() + geom_line(aes(x = date, y = 1e5*rolling_cases_last_30_days/owid_population, color = code)) + 
    xlab('Date') + ylab('Cases per 100k in last 30 days') + 
    ggtitle('Cases per 100k in previous 30 days') +theme_bw()




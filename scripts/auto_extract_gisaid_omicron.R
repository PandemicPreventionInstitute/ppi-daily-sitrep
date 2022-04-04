#SV Scarpino
#Autodownload meta-data from GISAID
#Dec 28th 2021

rm(list = ls())
USE_CASE = Sys.getenv("USE_CASE")
if(USE_CASE == ""){
    USE_CASE<-'local'
}
#USE_CASE = 'local' # 'domino' or 'local'
###########
#Libraries#
###########
if (USE_CASE == 'domino'){
install.packages("tidyverse", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("janitor", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("httr", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("countrycode", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("lubridate", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
}
if (USE_CASE == 'databricks'){
    install.packages("tidyverse", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
    install.packages("janitor", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
    install.packages("httr", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
    install.packages("countrycode", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
    install.packages("lubridate", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
}
library(httr)
library(tidyverse)
library(janitor)
library(countrycode)
library(lubridate)

#########
#Globals#
#########
if (USE_CASE =='domino'){
secrets <- read.csv("/mnt/data/secrets_gisaid.csv", header = FALSE) #a file with the username on the first row and password on the second row. No header
}
if (USE_CASE == 'local'){
    secrets <-read.csv("/tables/ppi-daily-sitrep/data/secrets_gisaid.csv")
}
if (USE_CASE =='local'){
    secrets <- read.csv("../data/secrets_gisaid.csv", header = FALSE) #a file with the username on the first row and password on the second row. No header
}
user <- as.character(secrets[1,1])
pw <- as.character(secrets[2,1])

stopifnot('username is not of type character'= is.character(user))

######################
#Download and process#
######################

#1. Download data
omicron_gisaid_xz <- GET("https://www.epicov.org/epi3/3p/rockfeed/export/N_Omicron_seq_by_country_day.csv.xz", 
    authenticate(user = user, password = pw, type = "basic"))
gisaid_metadata_xz <- GET("https://www.epicov.org/epi3/3p/rockfeed/export/GISAID_line_list_seq_data.csv.xz",
                          authenticate(user = user, password = pw, type = "basic"))


#2. Extract contents (the response object returned above has a lot of other entries)
omicron_gisaid_GET_content <- content(omicron_gisaid_xz)
gisaid_metadata_GET_content<- content(gisaid_metadata_xz)

#3. Decompress the raw data
omicron_gisaid_raw_text <- memDecompress(omicron_gisaid_GET_content, type = "xz", asChar = TRUE)
gisaid_metadata_raw_text <- memDecompress(gisaid_metadata_GET_content, type = "xz", asChar = TRUE)

#4. It's formated as a csv file. Use "text" instead of "file" because we don't want to open a new connection, just "translate" the existing plain text data.
omicron_gisaid <- read.csv(text = omicron_gisaid_raw_text)%>% 
    janitor::clean_names()%>%
    complete(country, submission_date)%>%
    rename(n = number_of_omicron_cases_submitted)%>%
    mutate(code = countrycode(country, origin = 'country.name', destination = 'iso3c'))

gisaid_metadata <- read.csv(text = gisaid_metadata_raw_text)

#5. Fix GISAID metadata load in (first row accidentally becomes column names)
first_row<-colnames(gisaid_metadata)
accession_id<- first_row[1]
country <-first_row[2]
location <- str_replace_all(first_row[3], "[.]"," ")
submission_date <- chartr(old = ".", new = "-", substr(first_row[4], 2, 11))
collection_date <- chartr(old = ".", new = "-", substr(first_row[5], 2, 11))
clade<-first_row[6]
pango_lineage<-first_row[7]
first_seq<-data.frame(accession_id, country, location, submission_date, collection_date, clade, pango_lineage)
#rename colnames
if (ncol(gisaid_metadata) == 7){
    colnames(gisaid_metadata)<- c("accession_id", "country", "location", "submission_date", "collection_date", "clade", "pango_lineage")
}
if (ncol(gisaid_metadata) == 8){
    colnames(gisaid_metadata)<- c("accession_id", "country", "location", "submission_date", "collection_date", "clade", "pango_lineage", "variant")
    gisaid_metadata<-gisaid_metadata%>%select(!variant)
}

gisaid_metadata<-rbind(first_seq, gisaid_metadata)

#6. Write both files to csvs
# Domino
if (USE_CASE == 'domino'){
write.csv(omicron_gisaid, '/mnt/data/raw/omicron_gisaid_feed.csv', row.names = FALSE)
write_csv(gisaid_metadata, '/mnt/data/raw/metadata.csv')
}
# local
if (USE_CASE == 'local'){
write.csv(omicron_gisaid, '../data/raw/omicron_gisaid_feed.csv', row.names = FALSE)
write_csv(gisaid_metadata, '../data/raw/metadata.csv')
}



#done!

# Let's do a quick check of BA.2
Denmark_recent_sequences<-gisaid_metadata[gisaid_metadata$country == "Denmark",]
n_BA_2s_in_Denmark = sum(Denmark_recent_sequences$pango_lineage == "BA.2")
US_recent_sequences<-gisaid_metadata[gisaid_metadata$country == "USA",]
n_BA_2s_in_US = sum(US_recent_sequences$pango_lineage == "BA.2")

only_BA2<-gisaid_metadata%>%filter(pango_lineage == "BA.2")


unique_pango_lineages<-unique(gisaid_metadata$pango_lineage)

unique_pango_lineages<-unique(gisaid_metadata$pango_lineage)

is_BA.2_present<-"BA.2" %in% unique_pango_lineages

if (USE_CASE == 'local'){
    write.csv(only_BA2, '../data/processed/BA2_submissions_from_feed.csv')
}

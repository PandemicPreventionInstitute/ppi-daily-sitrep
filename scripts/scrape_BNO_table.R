# This file scrapes the BNO table at https://bnonews.com/index.php/2021/11/omicron-tracker/

#set working dir
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# Libraries ---------------------------------------------------------------
rm(list = ls())
library(rvest)
library(tidyverse)
library(lubridate)

# Scrape webpage ----------------------------------------------------------

url <- paste0('https://docs.google.com/spreadsheets/d/e/2PACX-1vRi5YVV_L0XY-vORaD2h6-02',
            'bz9qfU6Kb-OovbmrvnvMi5zZSvqS-PPJybf0qgSWm2BLZlU6uFEjJPW/pubhtml?gid=2049345986')
page <- read_html(url)

# Process webpage html ----------------------------------------------------

xpath <- paste0("//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 'softmerge', ' ' ))] |",
          "//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 'softmerge-inner', ' ' ))]",
          "| //*[contains(concat( ' ', @class, ' ' ), concat( ' ', 's12', ' ' ))] | ",
          "//*[contains(concat( ' ', @class, ' ' ), concat( ' ', 's13', ' ' ))] | //*",
          "[contains(concat( ' ', @class, ' ' ), concat( ' ', 's10', ' ' ))] | //*",
          "[contains(concat( ' ', @class, ' ' ), concat( ' ', 's9', ' ' ))] | //*",
          "[contains(concat( ' ', @class, ' ' ), concat( ' ', 's8', ' ' ))] | //*",
          "[contains(concat( ' ', @class, ' ' ), concat( ' ', 's6', ' ' ))]")

raw_table <- html_elements(page, xpath = xpath) %>%
  html_text2 


# Convert html table to tibble --------------------------------------------

# remove leading and trailing text and convert from vector to n x 3 matrix
table_as_matrix <- matrix(raw_table[8:(length(raw_table)-2)], ncol = 3, byrow = T)


clean_table <- tibble(location = table_as_matrix[,1],
       confirmed = table_as_matrix[,2],
       probable = table_as_matrix[,3]) %>% 
  mutate(confirmed = as.integer(confirmed),
         probable = as.integer(probable))

# Fix country code -------------------------------------------------------------

# 
clean_table$location[clean_table$location=="Réunion (France)"]<-"Réunion"
clean_table<-clean_table%>%mutate(
    code = countrycode(location, origin = 'country.name', destination = 'iso3c'))
clean_table<-clean_table%>%filter(location!="TOTAL CASES", location !="CONFIRMED CASES", location!= "Confirmed")
table_states<-clean_table[is.na(clean_table$code),]
clean_table<-clean_table[!is.na(clean_table$code),]

# hack for USA
table_states<-table_states%>%mutate(
    code = 'USA')
USA_summary<-table_states%>%summarise(
    location = "United States of America",
    confirmed = sum(confirmed),
    probable = sum(probable),
    code = "USA"
)

clean_table<-rbind(clean_table, USA_summary)

# Save tibble -------------------------------------------------------------
write_csv(clean_table, '../data/raw/BNO_table.csv')



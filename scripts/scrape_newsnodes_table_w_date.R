# This file scrapes the BNO table at https://bnonews.com/index.php/2021/11/omicron-tracker/

# local set working dir
#setwd(dirname(rstudioapi::getActiveDocumentContext()$path))


# Domino wd
getwd()

# Libraries ---------------------------------------------------------------
rm(list = ls())

install.packages("rvest", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("tidyverse", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("lubridate", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("countrycode", dependencies=TRUE, repos='http://cran.us.r-project.org')

library(rvest)
library(tidyverse)
library(lubridate)
library(countrycode)

# Scrape webpage ----------------------------------------------------------

url <- paste0("https://newsnodes.com/nu_tracker")
page <- url %>%
    httr::GET(config = httr::config(ssl_verifypeer = FALSE)) %>%
    read_html()

# Process webpage html ----------------------------------------------------

xpath <- "//*[(@id = 'homecontent') and (((count(preceding-sibling::*) + 1) = 8) and parent::*)]//td"

raw_table <- html_elements(page, xpath = xpath) %>%
    html_text2 


# Convert html table to tibble --------------------------------------------

# remove leading and trailing text and convert from vector to n x 3 matrix
table_as_matrix <- matrix(raw_table[6:(which(raw_table == "total (worldwide)")-1)], ncol = 5, byrow = T)


clean_table <- tibble(location = table_as_matrix[,1],
                      confirmed = table_as_matrix[,2],
                      probable = table_as_matrix[,4]) %>% 
    mutate(confirmed = as.integer(gsub(',', '', confirmed)),
           probable = as.integer(gsub(',', '', probable)))

# Fix country code -------------------------------------------------------------

# 
clean_table$location[clean_table$location=='Réunion (France)']<-'Réunion'

# Split up the US states vs countries
# index_USA<-which(clean_table$location == 'TOTAL CASES')
# USA_table<-clean_table[(index_USA+3):nrow(clean_table),]
# clean_table<-clean_table[1:index_USA-1,]
clean_table<-clean_table%>%mutate(
    code = countrycode(location, origin = 'country.name', destination = 'iso3c'))



# Add time stamp to country table and USA table
clean_table<-clean_table%>% 
    mutate(timestamp = lubridate::now())
# USA_table<-USA_table%>% 
    #mutate(timestamp = lubridate::now())

# Save daily tibble -------------------------------------------------------------

date <- str_replace(substr(lubridate::now('EST'), 1, 13), ' ', '-')

# Domino path
write_csv(clean_table, paste0('/mnt/data/raw/daily_BNO_file/', date, '.csv'))
# write_csv(USA_table, paste0('/mnt/data/raw/daily_BNO_USA_file/', date, '.csv'))

# local path
# write_csv(clean_table, paste0('../data/raw/daily_BNO_file/', date, '.csv'))
# write_csv(USA_table, paste0('../data/raw/daily_BNO_USA_file/', date, '.csv'))

# Save updated master file ------------------------------------------------

# Domino path
master <- read_csv('/mnt/data/raw/BNO_scraped_master.csv')

# # local path
# master <- read_csv('../data/raw/BNO_scraped_master.csv')

master %>% 
    rbind(clean_table) %>% 
    #write_csv('../data/raw/BNO_scraped_master.csv') # local path
    write_csv('/mnt/data/raw/BNO_scraped_master.csv') # Domino path
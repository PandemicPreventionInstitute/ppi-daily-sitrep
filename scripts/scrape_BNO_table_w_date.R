# This file scrapes the BNO table at https://bnonews.com/index.php/2021/11/omicron-tracker/

# get working dir
getwd()

# Libraries ---------------------------------------------------------------
rm(list = ls())
install.packages("countrycode", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("rvest", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("lubridate", dependencies=TRUE, repos='http://cran.us.r-project.org')
install.packages("tidyverse", dependencies=TRUE, repos='http://cran.us.r-project.org')

library(rvest)
library(tidyverse)
library(lubridate)
library(countrycode)

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
    mutate(confirmed = as.integer(gsub(',', '', confirmed)),
           probable = as.integer(gsub(',', '', probable)))

# Fix country code -------------------------------------------------------------

# 
clean_table$location[clean_table$location=="Réunion (France)"]<-"Réunion"

# Split up the US states vs countries
index_USA<-which(clean_table$location == "TOTAL CASES")
USA_table<-clean_table[(index_USA+3):nrow(clean_table),]
clean_table<-clean_table[1:index_USA-1,]
clean_table<-clean_table%>%mutate(
    code = countrycode(location, origin = 'country.name', destination = 'iso3c'))



# Add time stamp to country table and USA table
clean_table<-clean_table%>% 
    mutate(timestamp = lubridate::now())
USA_table<-USA_table%>% 
    mutate(timestamp = lubridate::now())

# Save daily tibble -------------------------------------------------------------

date <- str_replace(substr(lubridate::now(), 1, 13), ' ', '-')

write_csv(clean_table, paste0('/mnt/data/raw/daily_BNO_file/', date, '.csv'))
# write_csv(USA_table, paste0('/mnt/data/raw/daily_BNO_USA_file/', data, '.csv'))

# Save updated master file ------------------------------------------------

master <- read_csv('/mnt/data/raw/BNO_scraped_master.csv')

master %>% 
    rbind(clean_table) %>% 
    write_csv('/mnt/data/raw/BNO_scraped_master.csv')
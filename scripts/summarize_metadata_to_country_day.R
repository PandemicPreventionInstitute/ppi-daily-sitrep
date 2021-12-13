# This file summarizes the GISAID metadata to country-day counts. It currently has no 
# filtering, except for ensuring human infection and double checking that lineage is Omicron
rm(list = ls())
# local set working dir
#setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# Domino
getwd()

# Libraries ---------------------------------------------------------------
install.packages("tidyverse", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("lubridate", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("janitor", dependencies = TRUE, repos = 'http://cran.us.r-project.org')
install.packages("countrycode", dependencies=TRUE, repos='http://cran.us.r-project.org')

library(tidyverse)
library(lubridate)
library(janitor)
library(countrycode)

# Data --------------------------------------------------------------------

today <- substr(lubridate::now('GMT'), 1, 13)
today <- chartr(old = '-', new = '_', today)
today <- chartr(old = ' ', new = '_', today)

# Domino path
path <- paste0('mnt/data/raw/gisaid_hcov-19_', today, '.tsv')
# local path
#path <- paste0('../data/raw/gisaid_hcov-19_', today, '.tsv')
 

x <- read_delim(path, delim = '\t') %>% 
                janitor::clean_names()

# Process data ------------------------------------------------------------

processed <- x %>% 
  #filter(host == "Human", lineage == 'B.1.1.529') %>% 
  separate(location, 
           into = c(NA, 'country'),
            sep = "[/]",
            extra = "drop") %>% 
  mutate(country = trimws(country)) %>% 
  group_by(country, submission_date) %>% 
  summarize(n = n()) %>% 
  ungroup() %>% 
  complete(country, submission_date)%>%
  mutate(code = countrycode(country, origin = 'country.name', destination = 'iso3c'))

# Save data ---------------------------------------------------------------

# local path
# write_csv(processed,'../data/processed/metadata_summarized.csv')

# Domino path
write_csv(processed,'/mnt/data/processed/metadata_summarized.csv')
  
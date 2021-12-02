# This file summarizes the GISAID metadata to country-day counts. It currently has no 
# filtering, except for ensuring human infection and double checking that lineage is Omicron
rm(list = ls())
#set working dir
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# Libraries ---------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(janitor)

# Data --------------------------------------------------------------------

today <- substr(lubridate::now('GMT'), 1, 13)
today <- chartr(old = '-', new = '_', today)
today <- chartr(old = ' ', new = '_', today)

path <- paste0('../data/raw/gisaid_hcov-19_', today, '.tsv')

x <- read_delim(path, delim = '\t') %>% 
                janitor::clean_names()

# Process data ------------------------------------------------------------

processed <- x %>% 
  filter(host == "Human", lineage == 'B.1.1.529') %>% 
  separate(location, 
           into = c(NA, 'country'),
            sep = "[/]",
            extra = "drop") %>% 
  mutate(country = trimws(country)) %>% 
  group_by(country, collection_date) %>% 
  summarize(n = n()) %>% 
  ungroup() %>% 
  complete(country, collection_date)%>%
  mutate(code = countrycode(country, origin = 'country.name', destination = 'iso3c'))

# Save data ---------------------------------------------------------------

write_csv(processed,'../data/processed/metadata_summarized.csv')
  
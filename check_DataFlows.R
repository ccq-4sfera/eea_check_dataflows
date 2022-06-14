# check that zones listed in file and in discodata are the same (no zones are missing)

library(renv)
# renv::init()
renv::restore()

library(tidyr)
library(tibble)
library(dplyr)
library(lubridate)
library(readxl)
library(RCurl)

source("./discodata.R")  #function to get results from discodata query

#TODO generate the url
file_url <- "https://cdr.eionet.europa.eu/Converters/run_conversion?file=es/eu/aqd/b/envysy9na/ES_B_Zones.xml&conv=530&source=remote"
# file_url <- "https://cdr.eionet.europa.eu/Converters/run_conversion?file=es/eu/aqd/c/envyszerg/ES_C_AssessmentRegime.xml&conv=530&source=remote"


# download file
temp.file <- paste0(tempfile(),".xls")
download.file(file_url, temp.file, mode = "wb")

# read and process file
file <- read_excel(temp.file, sheet = 'AirQualityZones')

# list zones in file
zones_in_file <- unique(file$LocalId)


# DISCODATA:
# SELECT TOP 100 * FROM [AirQualityDataFlows].[latest].[Zones]
# WHERE CountryCode = 'ES'
# AND ReportingYear = '2020'
disco <- discodata(FROM = "[AirQualityDataFlows].[latest].[Zones]",
                   CountryCode = "ES",
                   ReportingYear = 2020)


# list zones in discodata results
zones_in_disco <- unique(disco$ZoneId)


# find zones in file that are missing from discodata
missing_zones <- anti_join(file,disco,by=c("LocalId" = "ZoneId"))  #anti_join() returns all rows from x without a match in y

# if there are any missing, send alert
if(is.data.frame(missing_zones) & nrow(missing_zones)>0){
    print("Zones missing in discodata")
    
    print(missing_zones$LocalId)
    
    #TODO send alert
    
}else{
    print("OK!")
}


renv::snapshot()

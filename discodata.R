
library(httr)
# library(rjson)
library(jsonlite)
library(data.table)
library(dplyr)


#TODO SELECT only columns of interest (instead of SELECT *)

discodata <- function(FROM = "[AirQualityDataFlows].[latest].[AirQualityStatistics]",
                      SELECT = "*",
                      AirPollutant = "all",
                      DataAggregationProcessId = "all",
                      YearOfStatistics = "all",
                      CountryCode = c("ES"),
                      ReportingYear = 2020){


    # url="https://discodata.eea.europa.eu/sql?query=SELECT%20*%20FROM%20%5BAirQualityDataFlows%5D.%5Blatest%5D.%5BAirQualityStatistics%5D&p=1&nrOfHits=100&mail=null&schema=null"

    # PARAMETERS ---
    nrOfHits <- 10000
    
    # #Parameters used for AQ Report
    # # poll.list <- c("PM10", "PM2.5", "O3", "NO2", "BaP", "SO2", "CO", "C6H6", "As", "Cd", "Ni", "Pb")
    # aggreg.list <- c("P1Y","P1Y-P1D-per90.41","P1Y-dmax-per93.15","P1Y-dx-max","P1Y-P1D-per99",
    #                  "P1Y-day-max-per99.18","P1Y-hr-max-per99.79","P1Y-hrsAbove200","P1Y-hrsAbove350")
    # aggreg.list <- "all"
    # year.list <- 2000:2021 #e.g., 2020:2022, or "all"
    # 
    # # poll.list <- c('SO2','PM10','PM25','NO2','CO','O3') #e.g. c("NO2","O3") or "all"
    # 
    # country.list <- c("ES") #e.g. "AD", "ES" or "all"
    # ---
    
    
    poll.list <- AirPollutant
    aggreg.list <- DataAggregationProcessId
    year.list <- YearOfStatistics
    country.list <- CountryCode
    reporting.year.list <- ReportingYear

    
    #transform parameters selected to url format
    poll.list.char <- poll.list %>% paste(.,collapse="%27%2C%27") %>% gsub(" ", "%20",.)
    aggreg.list.char <- aggreg.list %>% paste(.,collapse="%27%2C%27")
    year.list.char <- year.list %>% paste(.,collapse="%2C")
    country.list.char <- country.list %>% paste(.,collapse="%27%2C%27")
    reporting.year.list.char <- reporting.year.list %>% paste(.,collapse="%2C")
    FROM.char <- FROM %>% gsub("[","%5B",.,fixed=T) %>% gsub("]","%5D",.,fixed=T)
    
    # each page contains 10000 data points, loop through pages until there is no more data
    for(p in 1:(nrOfHits/20)){
      print(paste0("page ",p))
        
      # generate query request ----
      url_query <- paste0("https://discodata.eea.europa.eu/sql?query=SELECT%20",SELECT,"%20FROM%20",FROM.char)
      
      # if at least one condition, add WHERE
      if(poll.list!="all" | aggreg.list!="all" | (TRUE %in% year.list!="all") | country.list!="all"){
        url_query <- paste0(url_query,"%0AWHERE%20")
      }
      
      if(poll.list!="all"){
        url_query <- paste0(url_query,"AirPollutant%20IN%20(%27",poll.list.char,"%27)")
      }
      if(aggreg.list!="all"){
        #if there was a condition for pollutant, do not add AND
        if(poll.list!="all"){
          url_query <- paste0(url_query,"%20%0AAND%20DataAggregationProcessId%20IN%20(%27",aggreg.list.char,"%27)")
        }else{
          #if this is the first condition, no AND
          url_query <- paste0(url_query,"DataAggregationProcessId%20IN%20(%27",aggreg.list.char,"%27)")
        }
      }
      if(year.list.char!="all"){
        #if this is not the first condition, do not add AND
        if(poll.list!="all" | aggreg.list!="all"){
          url_query <- paste0(url_query,"%20%0AAND%20YearOfStatistics%20IN%20(",year.list.char,")")
        }else{
          #if this is the first condition, no AND
          url_query <- paste0(url_query,"YearOfStatistics%20IN%20(",year.list.char,")")
        }
      }
      if(country.list!="all"){
        #if this is not the first condition, do not add AND
        if(poll.list!="all" | aggreg.list!="all" | (year.list.char!="all")){
          url_query <- paste0(url_query,"%20%0AAND%20CountryCode%20IN%20(%27",country.list.char,"%27)")
        }else{
          #if this is the first condition, no AND
          url_query <- paste0(url_query,"CountryCode%20IN%20(%27",country.list.char,"%27)")
        }
      }
      if(reporting.year.list.char!="all"){
        #if this is not the first condition, do not add AND
        if(poll.list!="all" | aggreg.list!="all" | (year.list.char!="all") | country.list!="all"){
            url_query <- paste0(url_query,"%20%0AAND%20ReportingYear%20IN%20(",reporting.year.list.char,")")
        }else{
          #if this is the first condition, no AND
          url_query <- paste0(url_query,"ReportingYear%20IN%20(",reporting.year.list.char,")")
        }
      }
      url_query <- paste0(url_query,"&p=",p,"&nrOfHits=",nrOfHits,"&mail=null&schema=null")
      
      
      # send query and get json ----
      # json_data <- fromJSON(file=url_query)
      json_data <- fromJSON(url_query)
      
      # convert json to readable data.table 
      df <- as.data.table(json_data[[1]])
      # df <- as.data.table(t(as.matrix(df)))
    
      # check if df is empty (last page was the last page with data)
      if(length(colnames(df))<=1){
        print("no more data")
        break
      }else{
        #if this page contains data, merge with data from previous pages
        # colnames(df) <- names(json_data[[1]][[1]])
        
        # merge different "pages"
        if(exists("df_all")){
          df_all <- df_all %>% rbind(df)
        }else{
          df_all <- df
        }
      }
    }
    
    
    return(df_all)
}

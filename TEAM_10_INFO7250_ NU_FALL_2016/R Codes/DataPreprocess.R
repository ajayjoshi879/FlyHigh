setwd("E:/Boston/NEU/Fall 2016/Additional/Scala/Scala_Project")
library("sqldf")
library(data.table)
library(plyr)
library(dplyr)

#Loading of the data
data_tkcarrier <- fread(input ="2015Q4/740680746_T_DB1B_MARKET.csv",header = TRUE)
data_carrier <- fread(input = "2015Q3/531708546_T_T100D_SEGMENT_ALL_CARRIER.csv",header = TRUE)

#Creating a lookup for the Airport Data
lookup <- fread(input ="2015Q2/531708546_T_MASTER_CORD.csv",header = TRUE)
uniquedata<- unique(lookup, by='AIRPORT_ID')

#Joining on the airport ID to get the latitude and longitude cordinates
dest_sample<-sqldf("select * from data_carrier as a inner join uniquedata as b on b.AIRPORT_ID=a.DEST_AIRPORT_ID")
origin_sample<-sqldf("select * from dest_sample as a inner join uniquedata as b on b.AIRPORT_ID=a.ORIGIN_AIRPORT_ID")
cordinates<-sqldf("select a.origin_airport_id,c.latitude as ORIGIN_LATITUDE,c.longitude as ORIGIN_LONGITUDE,a.dest_airport_id,b.latitude as DES_LATITUDE,b.longitude as DEST_LONGITUDE from data_carrier as a inner join uniquedata as b on b.AIRPORT_ID=a.DEST_AIRPORT_ID inner join uniquedata as c on c.AIRPORT_ID=a.ORIGIN_AIRPORT_ID")
data_input_cordinates <- cbind(origin_sample[,-c(4,9,15:22)],cordinates)
test_data <- data_tkcarrier[sample(nrow(data_tkcarrier),10000),]
set.seed(10000)
test_data2 <- data_input_cordinates[sample(nrow(data_carrier),10000),]

#Setting the name of the column from TICKET_CARRIER to CARRIER
setnames(test_data,"TICKET_CARRIER","CARRIER")
test.df <- sqldf("select * from test_data a INNER JOIN test_data2 b ON a.CARRIER=b.CARRIER")
set.seed(1000000)

#Splitting data to 500000 rows for 1 quarter
test.df <- test.df[sample(nrow(test.df),500000),]
test.df <- test.df[,-c(7)]
data<- read.csv(file = "2015_Quarter4.csv",header = TRUE,sep = ',')
#Trying to split the data into Day of the week
DAYS <- weekdays(as.Date(data$DATE_OF_TRAVEL,'%Y-%m-%d'))
library(zoo)
DATE <- as.Date((data$DATE_OF_TRAVEL),format = '%Y-%m-%d')

#Getting the month of the DATE OF TRAVEL
MONTH <- months(DATE)
data$MONTH <- MONTH
write.csv(data,"2015_Quarter4.csv",row.names = FALSE)
date_data <- cbind(date_data,DAYS,MONTH)
test.df <- test.df[,-c(1:2,5)]
test.df <- cbind(test.df,date_data)
Distance <- 0
library(geosphere)
for( i in 1:nrow(test.df)){
  Distance[i]<-distm (c(test.df$ORIGIN_LONGITUDE[i],test.df$ORIGIN_LATITUDE[i]), c(test.df$DEST_LONGITUDE[i],test.df$DES_LATITUDE[i]), fun = distVincentyEllipsoid)  
  
}
min <- min(Distance)
max <- max(Distance)
mean <- mean(Distance)
colnames(test.df)
test.df <- data.frame(test.df,Distance)

#Quarterly DATA file
write.csv(test.df,file = "2015_Quarter4.csv",row.names = FALSE)

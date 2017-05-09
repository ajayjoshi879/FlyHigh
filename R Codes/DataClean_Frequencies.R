
#set the current working directory
setwd("E:/Boston/NEU/Fall 2016/Additional/Scala/Scala_Project")

#read the csv file
data<- read.csv(file = "2015_Quarter4.csv",header = TRUE,sep = ',')
#set.seed(10000)
#index <- sample(1:nrow(data),round(0.75*nrow(data)))
train <- data
carrier<-unique(train$CARRIER,incomparables = FALSE)
library(plyr)
a<-count(train, "CARRIER")
Carrier_frequencies<-as.data.frame(a)
library(clusterSim)
Carrier_frequencies$Freq_Normalized <- data.Normalization(a$freq,type ="n1",normalization = "column")
write.csv(Carrier_frequencies,file = "Q4_carrierFrequencies.csv",row.names = FALSE)
library(sqldf)
train.new<-sqldf("select * from train t join Carrier_frequencies c on t.CARRIER=c.CARRIER")
train.new.1<-c("CARRIER","SEATS","ORIGIN","DEST","MONTH","MONTH_NUMBER","DAYS","DATE_OF_TRAVEL","BaseFare","TICKET_FARE","Freq_Normalized")
colnames(train.new)
require(lubridate)
train.new$DATE_OF_TRAVEL
#DATE_Q1 <- as.Date(train.new$DATE_OF_TRAVEL,format = "%Y-%m-%d")
train.new$DAYS <- day(as.Date(train.new$DATE_OF_TRAVEL,format = "%m/%d/%Y"))
train.new$MONTH_NUMBER <- month(as.Date(train.new$DATE_OF_TRAVEL,format = "%m/%d/%Y"))
write.csv(train.new[train.new.1],file = "validate_Q4.csv",row.names = FALSE)
library(data.table)
data_read <- read.csv(file = "validate_Q4.csv",sep = ',')
location <-unique(data_read$ORIGIN,incomparables = FALSE)
location1<-unique(data_read$DEST,incomparables = FALSE)
origin_count_df <- as.data.frame(location)
destin_count_df <- as.data.frame(location1)
setnames(destin_count_df,"location1","location")
location_df <- rbind(origin_count_df,destin_count_df)
location <- unique(location_df,incomparables = FALSE)
location <- as.data.frame(location)
location$Value <- 1:nrow(location)
write.csv(location,file = "location_Q4.csv")
#origin_score <- as.data.frame(data_read$ORIGIN)
#destin_score <- as.data.frame(data_read$DEST)
data_read_1 <- sqldf("select * from location d join data_read a on d.location=a.ORIGIN")
setnames(data_read_1,"Value","Origin_Value")
data_read_1<-data_read_1[,-c(1)]
data_read_2 <- sqldf("select * from location d join data_read_1 a on d.location=a.DEST")
setnames(data_read_2,"Value","Destin_Value")
Destin_Value_Normalized <- setnames(as.data.frame(scale(data_read_2$Destin_Value)),"V1","Destin_Value_Normalized")
Origin_Value_Normalized <- setnames(as.data.frame(scale(data_read_2$Origin_Value)),"V1","Origin_Value_Normalized")
data_read_2<-data_read_2[,-c(1)]
data_read_2 <- cbind(data_read_2,Destin_Value_Normalized,Origin_Value_Normalized)


data_read_2$Base_Fare_Normalized <- data.Normalization(data_read_2$BaseFare,type ="n1",normalization = "column")
data_read_2$Month_Standardization <- data.Normalization(data_read_2$MONTH_NUMBER,type ="n3",normalization = "column")
data_read_2$DAYS_Standardized <- data.Normalization(data_read_2$DAYS,type ="n3",normalization = "column")
data_read_2$Seats_Normalized <- data.Normalization(data_read_2$SEATS,type ="n3",normalization = "column")


write.csv(data_read_2,file = "validate_Q4.csv",row.names = FALSE)


#FOR PRICEFALL DETAILS
dopr <- read.csv(file = "validate_Q4.csv",header = TRUE,stringsAsFactors = FALSE)
dopr$DAY_OF_PRICEFALL <- day(as.Date(dopr$DATE_OF_PRICEFALL,format = "%m/%d/%Y"))
dopr$MONTH_NUMBER_OF_PRICEFALL <- month(as.Date(dopr$DATE_OF_PRICEFALL,format = "%m/%d/%Y"))
dopr$MONTH_OF_PRICEFALL <- months(as.Date(dopr$DATE_OF_PRICEFALL,format = "%m/%d/%Y"))
colnames(dopr)

write.csv(dopr,file = "validate_Q4.csv",row.names = FALSE)


#To generate complete dataset
csv1<- read.csv(file = "validate_Q1.csv",header = TRUE,sep = ',')
csv2<- read.csv(file = "validate_Q2.csv",header = TRUE,sep = ',')
csv3<- read.csv(file = "validate_Q3.csv",header = TRUE,sep = ',',stringsAsFactors = FALSE)
csv4<- read.csv(file = "validate_Q4.csv",header = TRUE,sep = ',')
colnames(csv1)
require(data.table)
setnames(csv2,"Freq_Normalized","CARRIER_Freq_Normalized")
setnames(csv4,"Freq_Normalized","CARRIER_Freq_Normalized")
setnames(csv4,"DAYS_OF_TRAVEL","DAY_OF_TRAVEL")
neural_data <- rbind(csv1,csv2,csv3,csv4)
dim(neural_data)
write.csv(neural_data,file="neural_data_normalized.csv",row.names = FALSE)
colnames(neural_data)

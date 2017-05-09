library(data.table)
setwd("E:/Boston/NEU/Fall 2016/Additional/Scala/Scala_Project")
data_airfare <- read.csv(file = "Input_Normalized.csv",sep = ',',stringsAsFactors = FALSE)
data_airfare <- data_airfare[,-c(1)]

index <- sample(1:nrow(data_airfare),round(0.75*nrow(data_airfare)))
train <- data_airfare[index,] 
test <- data_airfare[-index,]
#testing <- data[sample(nrow(data),200),]
formula <- as.formula("TICKET_FARE ~ MONTH_NUMBER_OF_TRAVEL+Carrier_Frequency_Normalized+Destin_Normalized+Origin_Normalized+Base_Fare_Normalized+Seats_Normalized")
#test_data <- testing[,c("TICKET_FARE","MONTH_NUMBER","DAYS","BaseFare","Carrier_Frequency_Normalized","Destin_Normalized","Origin_Normalized","Base_Fare_Normalized","Seats_Normalized")]
lm.fit <- lm(formula,data = train)
modelSummary <- summary(lm.fit)
modelSummary$r.squared
prediction <- as.data.frame(predict(lm.fit,test))
library(data.table)
colnames(prediction)
setnames(prediction,"predict(lm.fit, test)","Predicted_ticket_fare")
colnames(data_airfare)
data_frame <- cbind(prediction,test)
colnames(data_airfare)
write.csv(data_frame,file = "regression_prediction_2016.csv",row.names = FALSE)
actual_preds <- data.frame(cbind(actuals=test$TICKET_FARE,predicted=prediction))
correlation_accuracy <- cor(actual_preds)
head(actual_preds)

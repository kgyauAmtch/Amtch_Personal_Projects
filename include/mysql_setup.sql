CREATE DATABASE IF NOT EXISTS flightdb;
USE flightdb;
CREATE TABLE stagingflightprices(
`Airline` VARCHAR(50),
`Source` VARCHAR(50),
`Source_Name` VARCHAR(50),
`Destination` VARCHAR(50),
`Destination_Name` VARCHAR(255),
`Departure_Date_Time` DATETIME,
`Arrival_Date_Time` DATETIME,
`Duration_hrs` FLOAT,
`Stopovers` VARCHAR(50),
`Aircraft_Type` VARCHAR(255),
`Class` VARCHAR(50),
`Booking_Source` VARCHAR(255),
`Base_Fare_BDT` FLOAT,
`Tax_Surcharge_BDT` FLOAT,
`Total_Fare_BDT` FLOAT,
`Seasonality` VARCHAR(255),
`Days_Before_Departure` int
); 




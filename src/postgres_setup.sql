CREATE TABLE IF NOT EXISTS synthetic_data (
    Customer_id varchar(25) PRIMARY KEY,
    Product_id  varchar(25),
    Product_category varchar(25),
    Payment_type varchar(25),
    Device_Type varchar(25),
    Event_Type varchar(25),
    Event_date Date,
)


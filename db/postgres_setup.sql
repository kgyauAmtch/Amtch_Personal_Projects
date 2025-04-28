CREATE TABLE IF NOT EXISTS synthetic_data (
    customer_id varchar(25) PRIMARY KEY,
    product_id  varchar(25),
    product_category varchar(25),
    payment_type varchar(25),
    device_Type varchar(25),
    event_Type varchar(25),
    event_date Date
);


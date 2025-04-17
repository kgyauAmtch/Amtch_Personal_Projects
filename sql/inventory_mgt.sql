CREATE DATABASE `inventory_order_mgtsys`;
USE `inventory_order_mgtsys`;

/** Customers table  **/

CREATE TABLE customers (
    customer_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    -- last_name VARCHAR(50) NOT NULL,
    email VARCHAR(50) NOT NULL UNIQUE,
    phone_number VARCHAR(25)
);

/** products table  **/
CREATE TABLE products (
    product_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(50) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(65,2) NOT NULL,
    stock_quantity INT NOT NULL CHECK (stock_quantity >= 0) -- check this 
);

/** Orders table  **/

CREATE TABLE orders (
    order_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

/** Order Details table table  **/
CREATE TABLE order_details (
    order_detail_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    order_quantity INT NOT NULL CHECK (order_quantity > 0),
    order_price DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);


/** Inventory Logs table **/
CREATE TABLE inventory_logs (
    inventory_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    stock_change INT NOT NULL,
    log_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reason VARCHAR(100),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);


INSERT INTO customers (first_name, email, phone_number) VALUES ('Kwame', 'kwame@gmail.com', '0594854040');
INSERT INTO customers (first_name, email, phone_number) VALUES ('Kojo', 'kojo@gmail.com', '0244330919');
INSERT INTO customers (first_name, email, phone_number) VALUES ('Ama', 'amam@gmail.com', '0244330999');
INSERT INTO customers (first_name, email, phone_number) VALUES ('Sway', 'sway@gmail.com', '0244330873');
INSERT INTO customers (first_name, email, phone_number) VALUES ('Ray', 'ray@gmail.com', '0244330908');


INSERT INTO products (product_name, category, price, stock_quantity) VALUES ('Alienware', 'Electronics', 1200.00, 10);
INSERT INTO products (product_name, category, price, stock_quantity) VALUES ('Wireless Mouse', 'Accessories', 25.99, 50);
INSERT INTO products (product_name, category, price, stock_quantity) VALUES ('YSL', 'Perfume', 299.99, 20);
INSERT INTO products (product_name, category, price, stock_quantity) VALUES ('M4 competition', 'Car', 45.00, 4);
INSERT INTO products (product_name, category, price, stock_quantity) VALUES ('Lamp', 'Furniture', 30.50, 15);
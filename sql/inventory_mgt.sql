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

DELIMITER //

CREATE PROCEDURE process_order (
    IN p_customer_id INT,
    IN p_product_id INT,
    IN p_order_quantity INT
)
BEGIN
    DECLARE v_order_id INT;
    DECLARE v_price DECIMAL(10,2);
    DECLARE v_total_amount DECIMAL(10,2);

    -- Get the product price
    SELECT price INTO v_price
    FROM products
    WHERE product_id = p_product_id;

    -- Calculate total
    SET v_total_amount = v_price * p_order_quantity;

    -- Create the order
    INSERT INTO orders (customer_id, total_amount)
    VALUES (p_customer_id,  v_total_amount);

    SET v_order_id = LAST_INSERT_ID();

    -- Step 4: Add to order_details
    INSERT INTO order_details (
        order_id, product_id, order_quantity, order_price
    ) VALUES (
        v_order_id, p_product_id, p_order_quantity, v_price
    );

    -- Step 5: Deduct from stock
    UPDATE products
    SET stock_quantity = stock_quantity - p_order_quantity
    WHERE product_id = p_product_id;

    -- Step 6: Log the change in inventory
    INSERT INTO inventory_logs (
        product_id, stock_change, reason
    ) VALUES (
        p_product_id, -p_order_quantity, 'SALE'
    );
END //

DELIMITER ;

-- CALL process_order(2, 2, 2);
-- CALL process_order(1, 3, 4);


-- Track inventory changes (Trigger here because it runs everytime a stock level changes)
DELIMITER //

CREATE TRIGGER after_stock_update
AFTER UPDATE ON products
FOR EACH ROW
BEGIN
    DECLARE v_change INT;
    DECLARE v_reason VARCHAR(50);

    SET v_change = NEW.stock_quantity - OLD.stock_quantity;

    IF v_change != 0 THEN
        -- Determine the reason based on the direction of stock change
        IF v_change > 0 THEN
            SET v_reason = 'REPLENISHMENT';
        ELSE
            SET v_reason = 'SALE';
        END IF;

        -- Insert into inventory log with the cahnges
        INSERT INTO inventory_logs (product_id,stock_change,reason) VALUES (NEW.product_id,v_change,v_reason);
    END IF;
END //

DELIMITER ;

-- eg query to simulate a sale and replenishment to test the trigger
UPDATE products SET stock_quantity = stock_quantity - 5 WHERE product_id = 3;
UPDATE products SET stock_quantity = stock_quantity + 5 WHERE product_id = 3;



-- retrieve a history of inventory changes for auditing
DELIMITER //

CREATE PROCEDURE get_inventory_history()
BEGIN
    SELECT 
        il.inventory_id,
        p.product_name,
        il.stock_change,
        il.log_date,
        il.reason
    FROM 
        inventory_logs il
    JOIN 
        products p ON il.product_id = p.product_id
    ORDER BY 
        il.log_date DESC;
END //

DELIMITER ;

-- CALL get_inventory_history();


-- procedure to view particular customer order summary 
DELIMITER //

CREATE PROCEDURE get_customer_order_summary(IN p_customer_id INT)
BEGIN
    SELECT 
        o.order_id,
        o.order_date,
        o.total_amount,
        SUM(od.order_quantity) AS total_items
    FROM 
        orders o
    JOIN 
        order_details od ON o.order_id = od.order_id
    WHERE 
        o.customer_id = p_customer_id 
    GROUP BY 
        o.order_id, o.order_date, o.total_amount
    ORDER BY 
        o.order_date DESC;
END //

DELIMITER ;

CALL get_customer_order_summary(2);


-- Replenishment flagging

DELIMITER //

CREATE PROCEDURE get_products_to_replenish()
BEGIN
    SELECT 
        product_id,
        product_name,
        stock_quantity,
        5 AS reorder_point,
        'NEEDS REPLENISHMENT' AS situation 
    FROM 
        products
    WHERE 
        stock_quantity < 5
    ORDER BY 
        stock_quantity ASC;
END //

DELIMITER ;

-- CALL get_products_to_replenish();

-- spending tier of customers
DELIMITER //

CREATE PROCEDURE get_customer_spending_tier()
BEGIN
    SELECT 
        c.customer_id,
        c.first_name AS customer_name,
        c.email,
        SUM(o.total_amount) AS total_spent,
        CASE 
            WHEN SUM(o.total_amount) >= 1000 THEN 'Gold'
            WHEN SUM(o.total_amount) >= 500 THEN 'Silver'
            ELSE 'Bronze'
        END AS spending_tier
    FROM 
        customers c
    JOIN 
        orders o ON c.customer_id = o.customer_id
    GROUP BY 
        c.customer_id, c.first_name, c.email
    ORDER BY 
        total_spent DESC;
END //

DELIMITER ;

-- CALL get_customer_spending_tier()


-- Apllying bulk discount to customers
DELIMITER //

CREATE TRIGGER apply_bulk_discount
BEFORE INSERT ON order_details
FOR EACH ROW
BEGIN
    DECLARE discount_rate DECIMAL(5,2) DEFAULT 0;
    DECLARE unit_price DECIMAL(10,2);

    -- Get product price first
    SELECT price INTO unit_price FROM products WHERE product_id = NEW.product_id;

    -- Determine discount based on quantity
    IF NEW.order_quantity >= 50 THEN
        SET discount_rate = 0.15;
    ELSEIF NEW.order_quantity >= 20 THEN
        SET discount_rate = 0.10;
    ELSEIF NEW.order_quantity >= 10 THEN
        SET discount_rate = 0.05;
    ELSE
        SET discount_rate = 0.00;
    END IF;

    -- Apply discount
    SET NEW.order_price = NEW.order_quantity * unit_price * (1 - discount_rate);
END //

DELIMITER ;


-- query to test trigger
-- INSERT INTO order_details (order_id, product_id, order_quantity) VALUES (1, 2, 25);


-- Implement stock replenishment
DELIMITER //

CREATE PROCEDURE replenish_stock(
    IN p_product_id INT,
    IN p_quantity_added INT
)
BEGIN
    DECLARE current_stock INT;

    -- Get current stock
    SELECT stock_quantity INTO current_stock
    FROM products
    WHERE product_id = p_product_id;

    -- If below reorder point (5), replenish
    IF current_stock < 5 THEN
        -- Update product stock
        UPDATE products
        SET stock_quantity = stock_quantity + p_quantity_added
        WHERE product_id = p_product_id;

        -- Log the replenishment
        INSERT INTO inventory_logs (product_id, stock_change, reason)
        VALUES (p_product_id, p_quantity_added, 'Replenishment');
    END IF;
END //

DELIMITER ;
-- DROP PROCEDURE IF EXISTS replenish_stock;

-- CALL replenish_stock(4,20);


-- Automation 
-- A trigger that update stocks levels after an order is placed  

DELIMITER //

CREATE TRIGGER after_order_detail_insert
AFTER INSERT ON order_details
FOR EACH ROW
BEGIN
    -- Reduce product stock
    UPDATE products
    SET stock_quantity = stock_quantity - NEW.order_quantity
    WHERE product_id = NEW.product_id;

    -- Log the stock change
    INSERT INTO inventory_logs (product_id, stock_change, reason)
    VALUES (NEW.product_id, -NEW.order_quantity, 'Order placed');
END //

DELIMITER ;


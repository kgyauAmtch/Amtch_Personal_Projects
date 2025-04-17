
DELIMITER //
DROP PROCEDURE IF EXISTS process_order;

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


-- retrieve a history of inventory changes for auditing

DELIMITER //
DROP PROCEDURE IF EXISTS get_inventory_history;
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



-- procedure to view particular customer order summary 
DELIMITER //
DROP PROCEDURE IF EXISTS get_customer_order_summary;
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

-- CALL get_customer_order_summary(2);


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
DROP PROCEDURE IF EXISTS get_customer_spending_tier;
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


-- Implement stock replenishment
DELIMITER //
DROP PROCEDURE IF EXISTS replenish_stock;
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

-- CALL replenish_stock(4,20);
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


-- a Trigger that calculates total order amount

DELIMITER //

CREATE TRIGGER update_order_total
AFTER INSERT ON order_details
FOR EACH ROW
BEGIN
    UPDATE orders
    SET total_amount = (
        SELECT SUM(order_price)
        FROM order_details
        WHERE order_id = NEW.order_id
    )
    WHERE order_id = NEW.order_id;
END //

DELIMITER ;

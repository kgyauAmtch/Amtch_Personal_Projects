-- procedure call scripts
CALL process_order(2, 2, 2);
CALL process_order(1, 3, 4);

-- retrieve a history of inventory changes for auditing
CALL get_inventory_history();

-- procedure to view particular customer order summary 
CALL get_customer_order_summary(2);

-- Replenishment flagging
CALL get_products_to_replenish();

-- spending tier of customers
CALL get_customer_spending_tier()

-- Implement stock replenishment
CALL replenish_stock(4,20);

-- A view that summarizes order information
SELECT * FROM order_summary;

-- A view to show low stock quantity 
SELECT * FROM low_stock_products;


-- eg query to simulate a sale and replenishment to test the trigger
UPDATE products SET stock_quantity = stock_quantity - 5 WHERE product_id = 3;
UPDATE products SET stock_quantity = stock_quantity + 5 WHERE product_id = 3;


-- query to test trigger Applying bulk discount to customers
INSERT INTO order_details (order_id, product_id, order_quantity) VALUES (1, 2, 25);


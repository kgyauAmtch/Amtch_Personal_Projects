
-- A view that summarizes order information

CREATE VIEW order_summary AS
SELECT 
    o.order_id,
    c.first_name AS customer_name,
    o.order_date,
    o.total_amount,
    SUM(od.order_quantity) AS total_items
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_details od ON o.order_id = od.order_id
GROUP BY o.order_id, c.first_name, o.order_date, o.total_amount;

-- SELECT * FROM order_summary;

-- A view to show low stock quantity 
-- i made the assumption that any stock quantity below 5  indicates low stock 
CREATE VIEW low_stock_products AS
SELECT 
    product_id,
    product_name,
    category,
    stock_quantity
FROM products
WHERE stock_quantity < 5;

-- SELECT * FROM low_stock_products;

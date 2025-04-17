-- create idx_index_name on table_name(column or columns)
-- Orders Table Indexes
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date);

-- Order Details Table Indexes
CREATE INDEX idx_order_details_order_id ON order_details(order_id);
CREATE INDEX idx_order_details_product_id ON order_details(product_id);

-- Products Table Indexes
CREATE INDEX idx_products_product_id ON products(product_id);
CREATE INDEX idx_products_stock_quantity ON products(stock_quantity);

-- Customers Table Index
CREATE INDEX idx_customers_customer_id ON customers(customer_id);

-- Inventory Logs Table Indexes
CREATE INDEX idx_inventory_logs_product_id ON inventory_logs(product_id);


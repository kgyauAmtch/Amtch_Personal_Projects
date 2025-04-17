# Inventory & Order Management System 

##  Overview

This project is a **SQL-based inventory and order management system**.  It is designed for managing products, customer orders, stock levels, and tracking inventory changes with automation, integrity checks, and performance optimization.


## Features

### 1. **Tables**

- `customers`: Stores customer data
- `products`: Product inventory with stock quantity and price
- `orders`: Order records
- `order_details`: saves customer order details 
- `inventory_logs`: Tracks every stock change

### 2. **Stored Procedures**

- `process_order`: Handles new orders, calculates totals, updates stock
- `replenish_stock`: Automatically restocks low items and logs them
- `get_customer_order_summary`: Lists all orders by a customer
- `low_stock_report`: Flags products below reorder point (stock < 5)
- `customer_spending_report`: Categorizes customersby their spending (Bronze, Silver, Gold)

### 3. **Triggers**

- `after_order_detail_insert`: Reduces stock & logs change on order

### 4. **Views**

- `order_summary_view`: Shows order totals, customer
- `low_stock_view`: Lists all products below reorder point of 5



## Reports

- **Customer spending insights** with  tiers
- **Inventory audit logs** with date and action status of each logging



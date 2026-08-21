-- Enable logical replication
-- Create replication user for Debezium
CREATE ROLE replicator WITH SUPERUSER REPLICATION PASSWORD 'replicator_password' LOGIN;
GRANT pg_read_all_data TO replicator;

-- Grant permissions to replication user
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;

-- =========================================================
-- Kinetix Dynamic Pricing Engine - Database Schema
-- =========================================================

-- 1. Products Table (Enhanced with cost, inventory, and dynamic pricing fields)
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    cost DECIMAL(10,2) NOT NULL,           
    base_price DECIMAL(10,2) NOT NULL,     
    current_price DECIMAL(10,2) NOT NULL,  
    min_price DECIMAL(10,2) NOT NULL,      
    max_price DECIMAL(10,2) NOT NULL,      
    stock_level INT NOT NULL DEFAULT 100,  
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Orders Table (Enhanced to capture the price at time of purchase)
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES products(product_id),
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,     
    total_amount DECIMAL(10,2) NOT NULL,
    order_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Price History Table (Tracks every price change for elasticity analysis)
CREATE TABLE price_history (
    history_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES products(product_id),
    old_price DECIMAL(10,2) NOT NULL,
    new_price DECIMAL(10,2) NOT NULL,
    price_change_pct DECIMAL(5,2) NOT NULL,  
    demand_signal DECIMAL(5,2),              
    reason VARCHAR(200),                      
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Product Stats Table (Flink writes real-time aggregated features here)
CREATE TABLE product_stats (
    product_id INT,
    avg_quantity DOUBLE PRECISION,
    order_velocity INT,
    real_time_revenue DOUBLE PRECISION,
    peak_order_size INT,
    arpu DOUBLE PRECISION,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PRIMARY KEY (product_id, window_start)
);

-- 5. Seed Data (Realistic products with cost basis and guardrails)
INSERT INTO products (name, category, cost, base_price, current_price, min_price, max_price, stock_level) VALUES
    ('Wireless Mouse',      'Electronics', 8.00,   25.00, 25.00, 15.00,  45.00,  100),
    ('Mechanical Keyboard', 'Electronics', 35.00,  85.00, 85.00, 55.00,  150.00, 75),
    ('Ergonomic Chair',     'Furniture',   120.00, 300.00, 300.00, 200.00, 500.00, 30);

-- 6. Seed some initial orders (at base price)
INSERT INTO orders (product_id, quantity, unit_price, total_amount) VALUES
    (1, 2, 25.00, 50.00),
    (2, 1, 85.00, 85.00),
    (3, 1, 300.00, 300.00);

-- 7. Enable logical replication for Debezium CDC
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE price_history REPLICA IDENTITY FULL;
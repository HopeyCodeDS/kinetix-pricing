-- Enable logical replication
-- Create replication user for Debezium
CREATE ROLE replicator WITH SUPERUSER REPLICATION PASSWORD 'replicator_password' LOGIN;
GRANT pg_read_all_data TO replicator;

-- Create Schema and Tables
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    base_price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(100)
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES products(product_id),
    quantity INT NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    order_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed Data
INSERT INTO products (name, base_price, category) VALUES
('Wireless Mouse', 25.00, 'Electronics'),
('Mechanical Keyboard', 85.00, 'Electronics'),
('Ergonomic Chair', 300.00, 'Furniture');

INSERT INTO orders (product_id, quantity, total_amount) VALUES
(1, 2, 50.00),
(2, 1, 85.00),
(3, 1, 300.00);

-- Grant permissions to replication user
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;

-- Table for Flink to write real-time aggregated stats into
CREATE TABLE product_stats (
    product_id INT,
    avg_quantity DOUBLE PRECISION,
    order_velocity INT,               -- Count of separate orders
    real_time_revenue DOUBLE PRECISION, -- Sum of total_amount
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PRIMARY KEY (product_id, window_start)
);
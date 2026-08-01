from pyflink.table import EnvironmentSettings, TableEnvironment
import os
import time

def main():
    print("🚀 Initializing Flink Table Environment...")
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    # 1. Define the Kafka Source (Reading Debezium JSON)
    print("📡 Registering Kafka Source...")
    table_env.execute_sql("""
        CREATE TABLE kafka_orders (
            order_id INT,
            product_id INT,
            quantity INT,
            total_amount DECIMAL(10, 2),
            order_timestamp TIMESTAMP(3),
            WATERMARK FOR order_timestamp AS order_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'dbserver1.public.orders',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-pricing-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'debezium-json'
        )
    """)

    # 2. Define the Postgres Sink (Writing the aggregated stats)
    print("🗄️ Registering Postgres Sink...")
    table_env.execute_sql("""
        CREATE TABLE postgres_product_stats (
            product_id INT,
            avg_quantity DOUBLE,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PRIMARY KEY (product_id, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/kinetix_db',
            'table-name' = 'product_stats',
            'username' = 'postgres',
            'password' = 'supersecret',
            'sink.max-retries' = '3'
        )
    """)

    # 3. The Magic: Stateful Windowed Aggregation
    # Grouping by product_id and calculating the average quantity over a 1-minute tumbling window
    print("⚙️ Executing Stream Processing Pipeline (1-min tumbling window)...")
    result_table = table_env.sql_query("""
        SELECT 
            product_id,
            AVG(quantity) as avg_quantity,
            TUMBLE_START(order_timestamp, INTERVAL '1' MINUTE) as window_start,
            TUMBLE_END(order_timestamp, INTERVAL '1' MINUTE) as window_end
        FROM kafka_orders
        GROUP BY 
            product_id, 
            TUMBLE(order_timestamp, INTERVAL '1' MINUTE)
    """)

    # 4. Write to Postgres
    result_table.execute_insert("postgres_product_stats").wait()

if __name__ == '__main__':
    main()
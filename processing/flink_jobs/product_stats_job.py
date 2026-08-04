from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    print("🚀 Initializing Flink Table Environment...")
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

        # --- CRITICAL FIX: Explicitly declare required JARs for PyFlink ---
    jar_paths = [
        "file:///opt/flink/lib/flink-connector-jdbc-3.1.1-1.17.jar", 
        "file:///opt/flink/lib/postgresql-42.6.0.jar"
    ]
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars", ";".join(jar_paths)
    )
    print("📦 Loaded required JDBC JARs")

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

    # 3. Stateful Windowed Aggregation (10-second window for fast testing)
    print("⚙️ Executing Stream Processing Pipeline (10-sec tumbling window)...")
    result_table = table_env.sql_query("""
        SELECT 
            product_id,
            AVG(quantity) as avg_quantity,
            TUMBLE_START(order_timestamp, INTERVAL '10' SECOND) as window_start,
            TUMBLE_END(order_timestamp, INTERVAL '10' SECOND) as window_end
        FROM kafka_orders
        GROUP BY 
            product_id, 
            TUMBLE(order_timestamp, INTERVAL '10' SECOND)
    """)

    # 4. Write to Postgres
    print("💾 Writing to Postgres...")
    result_table.execute_insert("postgres_product_stats").wait()
    print("✅ Job completed successfully!")

if __name__ == '__main__':
    main()
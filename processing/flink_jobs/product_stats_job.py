from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    print('🚀 Initializing Flink Table Environment...')
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    jar_paths = [
        'file:///opt/flink/lib/flink-connector-jdbc-3.1.1-1.17.jar',
        'file:///opt/flink/lib/postgresql-42.6.0.jar'
    ]
    table_env.get_config().get_configuration().set_string('pipeline.jars', ';'.join(jar_paths))
    print('📦 Loaded required JDBC JARs')

    # 1. Read RAW JSON from Kafka 
    print('📡 Registering Raw Kafka JSON Source...')
    table_env.execute_sql('''
        CREATE TABLE kafka_orders_raw (
            payload STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'dbserver1.public.orders',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-pricing-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    ''')

    # 2. Create a View that extracts fields using robust JSON functions and Processing Time
    print('🔍 Creating Processing View...')
    table_env.execute_sql('''
        CREATE VIEW kafka_orders AS
        SELECT 
            CAST(JSON_VALUE(payload, '$.after.order_id') AS INT) AS order_id,
            CAST(JSON_VALUE(payload, '$.after.product_id') AS INT) AS product_id,
            CAST(JSON_VALUE(payload, '$.after.quantity') AS INT) AS quantity,
            CAST(JSON_VALUE(payload, '$.after.total_amount') AS DOUBLE) AS total_amount,
            PROCTIME() AS proc_time
        FROM kafka_orders_raw
        WHERE JSON_VALUE(payload, '$.op') = 'c'
    ''')

    # 3. Define the Postgres Sink
    print('🗄️ Registering Postgres Sink...')
    table_env.execute_sql('''
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
    ''')

    # 4. The Magic: Stateful Windowed Aggregation using Processing Time
    print('⚙️ Executing Stream Processing Pipeline (10-sec tumbling window)...')
    result_table = table_env.sql_query('''
        SELECT 
            product_id,
            AVG(quantity) as avg_quantity,
            TUMBLE_START(proc_time, INTERVAL '10' SECOND) as window_start,
            TUMBLE_END(proc_time, INTERVAL '10' SECOND) as window_end
        FROM kafka_orders
        GROUP BY 
            product_id, 
            TUMBLE(proc_time, INTERVAL '10' SECOND)
    ''')

    print('💾 Writing to Postgres...')
    result_table.execute_insert('postgres_product_stats').wait()
    print('✅ Job completed successfully!')

if __name__ == '__main__':
    main()
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    table_env.get_config().get_configuration().set_string(
        'pipeline.jars', 
        'file:///opt/flink/lib/flink-connector-jdbc-3.1.1-1.17.jar;file:///opt/flink/lib/postgresql-42.6.0.jar'
    )

    table_env.execute_sql('''
        CREATE TABLE kafka_orders_raw (payload STRING) WITH (
            'connector' = 'kafka', 'topic' = 'dbserver1.public.orders',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-pricing-group-v2',
            'scan.startup.mode' = 'latest-offset', 'format' = 'json'
        )
    ''')

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

    table_env.execute_sql('''
        CREATE TABLE postgres_product_stats (
            product_id INT, avg_quantity DOUBLE, order_velocity INT,
            real_time_revenue DOUBLE, peak_order_size INT, arpu DOUBLE, window_start TIMESTAMP(3), window_end TIMESTAMP(3),
            PRIMARY KEY (product_id, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc', 'url' = 'jdbc:postgresql://postgres:5432/kinetix_db',
            'table-name' = 'product_stats', 'username' = 'postgres', 'password' = 'supersecret'
        )
    ''')

    # NEW LOGIC: Added MAX(quantity) and Revenue/Velocity
    result_table = table_env.sql_query('''
        SELECT 
            product_id,
            CAST(AVG(quantity) AS DOUBLE) as avg_quantity,
            CAST(COUNT(order_id) AS INT) as order_velocity,
            CAST(SUM(total_amount) AS DOUBLE) as real_time_revenue,
            CAST(MAX(quantity) AS INT) as peak_order_size,
            CAST(SUM(total_amount) / COUNT(order_id) AS DOUBLE) as arpu,
            TUMBLE_START(proc_time, INTERVAL '10' SECOND) as window_start,
            TUMBLE_END(proc_time, INTERVAL '10' SECOND) as window_end
        FROM kafka_orders
        GROUP BY 
            product_id, 
            TUMBLE(proc_time, INTERVAL '10' SECOND)
    ''')

    result_table.execute_insert('postgres_product_stats').wait()

if __name__ == '__main__':
    main()
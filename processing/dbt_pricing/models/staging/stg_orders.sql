-- This reads from the source database
{{ config(
    materialized='external',
    file_format='parquet',
    location='s3://kinetix-lakehouse/bronze/orders/orders.parquet'
) }}

SELECT 
    order_id,
    product_id,
    quantity,
    total_amount,
    order_timestamp,
    CURRENT_TIMESTAMP as _loaded_at
FROM postgres_scan('host=127.0.0.1 port=5433 dbname=kinetix_db user=postgres password=supersecret', 'public', 'orders')
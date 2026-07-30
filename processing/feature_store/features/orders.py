import os
from datetime import timedelta
from feast import Entity, FeatureView, Field, ValueType
from feast.types import Int32, Float32
from feast.infra.offline_stores.file_source import FileSource

# 1. Define the Entity
order = Entity(
    name="order_id",
    value_type=ValueType.INT32,
    description="Unique identifier for an order",
)

# 2. Define the Data Source
current_dir = os.path.dirname(os.path.abspath(__file__))
local_parquet_path = os.path.abspath(os.path.join(current_dir, "..", "..", "dbt_pricing", "bronze_orders.parquet"))

print(f"DEBUG: Feast is looking for the parquet file at: {local_parquet_path}")

orders_source = FileSource(
    name="orders_source",
    path=local_parquet_path,
    timestamp_field="order_timestamp",
    created_timestamp_column="_loaded_at",
)

# 3. Define the Feature View
orders_feature_view = FeatureView(
    name="order_features",
    entities=[order],
    ttl=timedelta(days=1),
    schema=[
        Field(name="product_id", dtype=Int32),
        Field(name="quantity", dtype=Int32),
        Field(name="total_amount", dtype=Float32),
    ],
    source=orders_source,
)
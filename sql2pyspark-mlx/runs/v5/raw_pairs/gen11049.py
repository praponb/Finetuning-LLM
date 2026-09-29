from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "outer")
    .select(
        F.col("orders.status"),
        F.col("stores.store_name"),
        F.col("stores.opened_date"),
        F.col("customers.segment"),
    )
)

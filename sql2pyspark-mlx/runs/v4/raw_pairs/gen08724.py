from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("orders.shipping_fee"),
        F.col("stores.store_name"),
        F.col("customers.age"),
        F.col("customers.city"),
    )
)

from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("orders.status").isin(["PENDING", "CANCELLED"]) & (F.col("orders.discount") <= 0.16))
    .select(
        F.col("orders.id"),
        F.col("orders.store_id"),
        F.col("stores.region"),
        F.col("customers.age"),
    )
)

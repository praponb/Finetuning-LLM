from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("customers.is_active") & (F.col("customers.age") <= 64))
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("customers.city"),
        F.col("stores.opened_date"),
        F.col("stores.city").alias("store_city"),
    )
    .orderBy("status")
)

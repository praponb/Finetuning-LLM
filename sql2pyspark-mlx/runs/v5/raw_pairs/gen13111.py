from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .filter(F.col("customers.email").isNotNull() & (F.year(F.col("orders.order_date")) == 2023))
    .select(
        F.col("orders.channel"),
        F.col("orders.status"),
        F.col("customers.name"),
        F.col("customers.city"),
        F.col("stores.city").alias("store_city"),
    )
)

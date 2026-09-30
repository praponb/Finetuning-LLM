from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("orders.amount"),
        F.col("orders.discount"),
        F.col("customers.age"),
        F.col("customers.is_active"),
        F.col("stores.store_name"),
        F.col("stores.region"),
    )
    .orderBy(F.col("region").desc())
)

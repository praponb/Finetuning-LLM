from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .select(
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("c.segment"),
        F.col("c.name"),
    )
    .orderBy("amount")
)

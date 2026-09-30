from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("c.country"),
    )
    .orderBy("discount")
)

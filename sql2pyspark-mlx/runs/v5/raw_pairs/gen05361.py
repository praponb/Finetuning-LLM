from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.city") != "Singapore")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("c.segment"),
    )
    .orderBy(F.col("segment").desc(), "id")
    .limit(50)
)

from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("c.country"),
        F.col("c.city"),
        F.col("c.is_active"),
    )
    .orderBy("city", F.col("id").desc())
)

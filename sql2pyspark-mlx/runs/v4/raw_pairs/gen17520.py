from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"),
        F.sum(F.when(F.col("o.channel") == "web", 1).otherwise(0)).alias("web_count"),
    )
    .orderBy("city", F.col("avg_discount").desc())
)

from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "right")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .groupBy(
        F.when(F.col("s.sqft") >= 12997, "high").when(F.col("s.sqft") >= 7643, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.avg(F.col("p.amount")).alias("avg_amount"))
    .orderBy("sqft_bucket")
)

from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .groupBy(
        F.when(F.col("s.sqft") >= 13463, "high").when(F.col("s.sqft") >= 6366, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.round(F.avg(F.col("o.amount")), 2).alias("avg_amount"))
    .orderBy("sqft_bucket")
)

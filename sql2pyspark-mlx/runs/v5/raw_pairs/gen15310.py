from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.sqft") >= 13835, "high").when(F.col("s.sqft") >= 3719, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.count(F.col("o.discount")).alias("discount_count"))
    .orderBy("sqft_bucket")
    .limit(5)
)

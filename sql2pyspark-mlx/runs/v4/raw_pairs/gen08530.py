from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.37, "high").when(F.col("discount") >= 0.12, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.max("order_date").alias("latest_order_date"))
    .orderBy("latest_order_date", "discount_bucket")
    .limit(3)
)

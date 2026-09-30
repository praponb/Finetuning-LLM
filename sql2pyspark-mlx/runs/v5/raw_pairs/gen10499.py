from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("discount") >= 0.36, "high").when(F.col("discount") >= 0.19, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "PENDING", F.col("discount")).otherwise(0)).alias("pending_discount"),
        F.min("order_date").alias("first_order_date"),
    )
    .filter(F.col("pending_discount") < 80)
    .select("discount_bucket", "pending_discount", "first_order_date")
    .orderBy(F.col("discount_bucket").desc())
)

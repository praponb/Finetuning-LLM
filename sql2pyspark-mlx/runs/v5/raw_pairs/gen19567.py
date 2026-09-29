from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("amount").between(1080, 1460))
    .withColumn("order_date_month_end", F.last_day("order_date"))
)

orders_stats = (
    filtered_orders
    .groupBy(
        F.when(F.col("discount") >= 0.35, "high").when(F.col("discount") >= 0.16, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.countDistinct("discount").alias("unique_customers"),
    )
)

result = (
    orders_stats
    .filter(F.col("unique_customers") >= 4)
    .select("discount_bucket", "stddev_discount", "unique_customers")
    .orderBy(F.col("stddev_discount").desc(), "unique_customers", "discount_bucket")
    .limit(10)
)

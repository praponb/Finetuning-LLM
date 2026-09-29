from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("store_id").isin([5, 15, 17, 30]))
    .withColumn("order_date_period", F.date_trunc("WEEK", "order_date"))
)

amount_bucket_summary = (
    filtered_orders
    .groupBy(
        F.when(F.col("amount") >= 1280, "high").when(F.col("amount") >= 380, "mid").otherwise("low").alias("amount_bucket"),
        F.year("order_date").alias("order_year"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    amount_bucket_summary
    .select("amount_bucket", "order_year", "avg_amount", "min_amount")
)

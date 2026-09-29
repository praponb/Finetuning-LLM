from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"), "channel")
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.max("amount").alias("max_amount"),
    )
)

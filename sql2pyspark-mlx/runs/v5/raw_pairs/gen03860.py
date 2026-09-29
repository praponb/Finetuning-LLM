from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("amount") >= 1640, "high").when(F.col("amount") >= 370, "mid").otherwise("low").alias("amount_bucket"),
        F.date_format("order_date", "yyyy-MM").alias("order_month"),
    )
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") > 680)
    .orderBy(F.col("amount_bucket").desc())
)

from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(960, 970))
    .groupBy("method", F.month("paid_at").alias("paid_month"))
    .agg(
        F.count("amount").alias("amount_count"),
        F.countDistinct("amount").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") >= 4)
)

from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .groupBy(F.to_date("paid_at").alias("paid_day"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy(F.col("min_amount").desc())
)

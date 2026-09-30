from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") >= "2024-04-01") & F.col("order_id").isNull() & (F.col("amount") > 890))
    .groupBy("status")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy("status")
)

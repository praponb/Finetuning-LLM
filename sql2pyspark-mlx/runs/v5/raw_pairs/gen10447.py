from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.max("paid_at").alias("latest_paid_at"),
    )
    .filter(F.col("stddev_amount") > 63)
    .select("method", "stddev_amount", "latest_paid_at")
)

from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1030, 1120))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"), "status")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") >= 33)
    .select("paid_quarter", "status", "stddev_amount")
)

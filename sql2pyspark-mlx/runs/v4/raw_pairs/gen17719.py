from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("status") == "REFUNDED") & (F.col("paid_at") >= "2023-10-01"))
)

summary = (
    base
    .groupBy("status", F.hour("paid_at").alias("paid_hour"))
    .agg(F.avg("amount").alias("avg_amount"))
)

result = summary.orderBy(F.col("status").desc())

from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("method") == "promptpay") & (F.col("amount") < 480))
    .withColumn("method_upper", F.upper("method"))
)

summary = (
    base
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
)

result = summary.select("paid_hour", "min_paid_at", "refunded_count")

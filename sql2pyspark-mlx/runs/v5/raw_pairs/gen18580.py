from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.to_date("paid_at") < "2023-04-01")
    .withColumn("amount_rounded", F.round("amount", 2))
)

agg = (
    src
    .groupBy("status", "method")
    .agg(
        F.avg("amount_rounded").alias("avg_amount_rounded"),
        F.min("paid_at").alias("min_paid_at"),
    )
)

result = (
    agg
    .filter(F.col("avg_amount_rounded") < 58)
    .select("status", "method", "avg_amount_rounded")
)

from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= "2025-09-27") & (F.col("method") == "card"))
    .withColumn("amount_whole", F.floor("amount"))
)

summary = (
    base
    .groupBy(
        F.when(F.col("amount_whole") >= 62, "high").when(F.col("amount_whole") >= 27, "mid").otherwise("low").alias("amount_whole_bucket"),
    )
    .agg(F.avg("amount_whole").alias("avg_amount_whole"))
)

result = summary.select("amount_whole_bucket", "avg_amount_whole")

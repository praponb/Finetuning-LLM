from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1530, "high").when(F.col("amount") >= 710, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") < 440)
    .orderBy(F.col("avg_amount").desc(), "amount_bucket")
    .limit(5)
)

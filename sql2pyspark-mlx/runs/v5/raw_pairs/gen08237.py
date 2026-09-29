from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 1800)
    .groupBy(
        F.when(F.col("amount") >= 3100, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") > 3200)
)

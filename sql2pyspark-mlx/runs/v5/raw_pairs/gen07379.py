from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3000, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") > 77)
)

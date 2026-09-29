from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 1900)
    .groupBy(
        F.when(F.col("amount") >= 3800, "high").when(F.col("amount") >= 800, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .orderBy(F.col("avg_amount").desc())
)

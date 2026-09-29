from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3900, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
        "account_id",
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("avg_amount") < 44)
    .select("amount_bucket", "account_id", "avg_amount", "max_amount")
    .orderBy(F.col("account_id").desc())
)

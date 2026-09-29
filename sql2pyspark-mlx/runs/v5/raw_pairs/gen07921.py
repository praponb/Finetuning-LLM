from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "is_flagged",
        F.when(F.col("amount") >= 3400, "high").when(F.col("amount") >= 1500, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("amount").alias("amount_count"))
    .filter(F.col("amount_count") >= 8)
    .orderBy("amount_bucket")
)

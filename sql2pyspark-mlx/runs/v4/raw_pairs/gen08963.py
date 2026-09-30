from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") <= 1100) & (F.col("amount") > 1800) & (F.col("merchant") == "Starbucks"))
    .groupBy(
        F.when(F.col("amount") >= 4200, "high").when(F.col("amount") >= 1100, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("amount").alias("amount_count"))
    .orderBy(F.col("amount_count").desc())
)

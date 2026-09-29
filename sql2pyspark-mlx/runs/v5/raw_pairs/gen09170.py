from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "is_flagged",
        F.when(F.col("amount") >= 4100, "high").when(F.col("amount") >= 1600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Starbucks", 1).otherwise(0))) / F.count("*")), 1).alias("pct_starbucks"),
    )
    .filter(F.col("total_amount") > 7000)
    .select("is_flagged", "amount_bucket", "total_amount", "pct_starbucks")
)

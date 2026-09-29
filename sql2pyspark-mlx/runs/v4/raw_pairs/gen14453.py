from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4200, "high").when(F.col("amount") >= 1300, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shell"),
        F.min("amount").alias("min_amount"),
    )
)

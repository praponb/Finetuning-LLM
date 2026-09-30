from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.min("balance").alias("min_balance"),
        F.count("balance").alias("balance_count"),
    )
    .filter(F.col("balance_count") < 8)
    .select("status", "min_balance", "balance_count")
    .orderBy(F.col("balance_count").desc())
)

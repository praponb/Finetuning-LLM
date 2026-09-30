from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
        F.count("*").alias("account_count"),
    )
    .filter(F.col("pct_open") < 23)
    .select("status", "pct_open", "account_count")
    .orderBy(F.col("account_count").desc())
)

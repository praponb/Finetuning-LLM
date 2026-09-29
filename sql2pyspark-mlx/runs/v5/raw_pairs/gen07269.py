from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("balance") <= 16000) & (F.col("account_type") == "CREDIT"))
)

accounts_stats = (
    base
    .groupBy("status")
    .agg(F.count("*").alias("total_count"))
)

result = (
    accounts_stats
    .filter(F.col("total_count") >= 9)
    .select("status", "total_count")
    .orderBy(F.col("total_count").desc(), "status")
    .limit(50)
)

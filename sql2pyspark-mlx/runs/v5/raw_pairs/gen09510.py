from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("account_type") != "LOAN") & (F.col("opened_date") < "2025-10-01"))
)

agg = (
    filtered_accounts
    .groupBy("status")
    .agg(
        F.count("balance").alias("balance_count"),
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
    )
)

result = (
    agg
    .select("status", "balance_count", "closed_count")
    .orderBy("closed_count", "status")
    .limit(5)
)

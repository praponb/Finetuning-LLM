from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]) & (F.col("opened_date") >= "2024-12-15")) | F.col("customer_id").isNull())
)

accounts_stats = (
    valid_accounts
    .groupBy("account_type", "status")
    .agg(F.max("balance").alias("max_balance"))
)

result = (
    accounts_stats
    .select("account_type", "status")
    .orderBy("status", "account_type")
    .limit(50)
)

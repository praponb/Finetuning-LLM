from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .filter(F.col("accounts.opened_date") < "2023-04-01")
    .groupBy(F.col("accounts.id"), F.col("accounts.status"))
    .agg(
        F.count(F.col("transactions.id")).alias("transaction_count"),
        F.sum(F.when(F.col("transactions.merchant") == "Starbucks", 1).otherwise(0)).alias("starbucks_count"),
    )
    .orderBy("id", "status")
    .limit(10)
)

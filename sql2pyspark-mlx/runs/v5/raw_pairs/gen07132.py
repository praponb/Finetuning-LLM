from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("merchant").isin(["Grab", "Starbucks", "Lazada"])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.status").isNotNull())
    .select(F.col("a.id"), F.col("a.balance"))
)

from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.to_date(F.col("t.txn_time")) >= "2025-07-01")
    .groupBy(F.col("a.status"))
    .agg(
        F.sum(F.when(F.col("t.merchant") == "Lazada", 1).otherwise(0)).alias("lazada_count"),
    )
    .orderBy("status")
)

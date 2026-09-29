from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0)).alias("lazada_count"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.lazada_count") >= 46)
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("t.lazada_count"),
        F.col("t.starbucks_count"),
    )
    .orderBy("id", F.col("starbucks_count").desc())
)

from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("as.unique_merchants"),
        F.col("as.merchant_list"),
    )
    .orderBy("account_type", F.col("id").desc())
)

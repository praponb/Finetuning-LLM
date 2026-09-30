from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.amount_count") >= 6)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("t.txn_type_list"),
        F.col("t.amount_count"),
    )
    .orderBy(F.col("amount_count").desc(), F.col("id").desc())
    .limit(3)
)

from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .select(F.col("a.id"), F.col("a.status"), F.col("A.txn_type_values"))
    .orderBy(F.col("status").desc())
)

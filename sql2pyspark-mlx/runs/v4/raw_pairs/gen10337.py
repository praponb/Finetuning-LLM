from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.to_date("txn_time") < "2023-03-14")
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .filter(F.col("as.amount_count") > 4)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("as.amount_count"),
        F.col("as.txn_type_list"),
    )
)

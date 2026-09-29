from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("A.txn_type_values"),
        F.col("A.total_amount"),
    )
)

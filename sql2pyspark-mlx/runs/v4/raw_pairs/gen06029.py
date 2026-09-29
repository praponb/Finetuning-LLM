from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .select(F.col("a.customer_id"), F.col("a.account_type"), F.col("A.txn_type_values"))
)

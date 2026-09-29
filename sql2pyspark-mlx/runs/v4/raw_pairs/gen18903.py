from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("tba.txn_type_values"))
    .orderBy("txn_type_values", "id")
    .limit(3)
)

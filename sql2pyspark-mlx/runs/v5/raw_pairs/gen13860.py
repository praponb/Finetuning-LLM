from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.opened_date") < "2024-08-06")
    .groupBy(F.col("a.status"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("t.txn_type")))).alias("txn_type_values"),
        F.max(F.col("t.amount")).alias("max_amount"),
    )
    .orderBy("txn_type_values", F.col("max_amount").desc())
)

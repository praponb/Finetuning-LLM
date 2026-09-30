from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("merchant") == "Shell")
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .filter(F.col("tba.min_amount") >= 2300)
    .select(
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("tba.txn_type_values"),
        F.col("tba.min_amount"),
    )
    .orderBy(F.col("customer_id").desc(), F.col("txn_type_values").desc())
)

from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.amount_range") >= 62)
    .select(
        F.col("a.account_type"),
        F.col("at.amount_range"),
        F.col("at.txn_type_values"),
    )
    .orderBy("txn_type_values", "account_type")
)

from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.customer_id"), F.col("t.merchant_values"))
    .orderBy(F.col("customer_id").desc())
)

from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("amount") > 1400)
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("at.merchant_values"),
    )
)

from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(~F.col("merchant").isin(["Grab", "Shell", "Lazada"]))
    .groupBy("account_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("T"), F.col("a.id") == F.col("T.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("T.max_amount"),
        F.col("T.merchant_values"),
    )
    .orderBy(F.col("account_type").desc())
)

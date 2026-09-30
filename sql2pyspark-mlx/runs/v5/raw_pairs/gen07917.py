from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("txn_time") < "2024-08-01")
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount")).otherwise(0)).alias("grab_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.grab_amount") >= 36)
    .select(
        F.col("accounts.id"),
        F.col("accounts.balance"),
        F.col("t.grab_amount"),
        F.col("t.merchant_values"),
    )
)

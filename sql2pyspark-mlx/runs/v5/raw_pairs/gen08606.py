from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0)).alias("grab_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.grab_count") > 49)
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("as.merchant_values"),
        F.col("as.grab_count"),
    )
    .orderBy(F.col("merchant_values").desc())
)

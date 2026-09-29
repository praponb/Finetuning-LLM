from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("amount") >= 2800)
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount")).otherwise(0)).alias("grab_amount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.amount_count") > 9)
    .select(
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("t.grab_amount"),
        F.col("t.amount_count"),
    )
    .orderBy(F.col("balance").desc())
)

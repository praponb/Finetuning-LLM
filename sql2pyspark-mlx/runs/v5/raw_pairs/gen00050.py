from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy("account_id")
    .agg(
        F.countDistinct("amount").alias("unique_accounts"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.total_amount") > 20000)
    .select(F.col("a.status"), F.col("as.unique_accounts"), F.col("as.total_amount"))
    .orderBy(F.col("unique_accounts").desc())
)

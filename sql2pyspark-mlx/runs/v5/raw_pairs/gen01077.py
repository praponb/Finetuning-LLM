from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(~F.col("a.status").isin(["OPEN", "FROZEN"]))
    .select(
        F.col("a.balance"),
        F.col("t.merchant"),
        F.col("t.is_flagged"),
        F.coalesce(F.col("a.balance"), F.lit(0)).alias("balance_filled"),
    )
    .orderBy(F.col("balance_filled").desc())
)

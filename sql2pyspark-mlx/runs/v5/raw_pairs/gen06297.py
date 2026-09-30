from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(F.col("a.status"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.merchant") == "Amazon", 1).otherwise(0))) / F.count("*")), 1).alias("pct_amazon"),
    )
    .filter(F.col("pct_amazon") > 67)
)

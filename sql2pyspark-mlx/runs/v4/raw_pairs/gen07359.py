from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.is_flagged"))
    .groupBy(F.year(F.col("a.opened_date")).alias("opened_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("t.txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
    )
    .filter(F.col("pct_refund") > 45)
    .orderBy(F.col("pct_refund").desc())
)

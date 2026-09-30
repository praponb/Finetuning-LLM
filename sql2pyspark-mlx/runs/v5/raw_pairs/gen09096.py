from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("t.is_flagged"),
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.date_trunc("YEAR", F.col("a.opened_date")).alias("opened_date_period"),
    )
    .orderBy(F.col("id").desc(), F.col("amount").desc())
)

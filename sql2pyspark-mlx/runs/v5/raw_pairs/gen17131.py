from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.is_flagged") | F.col("a.customer_id").isNull())
    .select(
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("t.is_flagged"),
        F.date_trunc("MONTH", F.col("a.opened_date")).alias("opened_date_period"),
    )
    .orderBy(F.col("customer_id").desc())
)

from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .filter(F.col("c.country").isin(["TH", "SG"]) & (F.col("c.segment") == "Corporate"))
    .select(
        F.col("t.txn_time"),
        F.col("t.is_flagged"),
        F.col("a.opened_date"),
        F.col("c.segment"),
    )
)

from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.is_flagged"),
        F.col("a.account_type"),
        F.col("c.name"),
        F.col("c.country"),
    )
    .orderBy("account_type", "country")
)

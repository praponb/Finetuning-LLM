from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("c.city"),
        F.col("c.is_active").cast("int").alias("is_active_flag"),
    )
    .orderBy("status")
)

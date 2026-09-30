from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("customer_id") == 16).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("t.is_flagged"),
        F.col("t.merchant"),
    )
)

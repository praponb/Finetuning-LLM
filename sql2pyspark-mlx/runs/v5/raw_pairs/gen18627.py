from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .filter(F.col("a.account_type") == "CREDIT")
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.amount"),
        F.col("t.account_id"),
        F.col("a.opened_date"),
        F.col("a.customer_id"),
        F.col("a.balance"),
    )
    .orderBy("merchant")
)

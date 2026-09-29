from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.customer_id").isin([2, 9, 18]))
    .select(
        F.col("t.amount"),
        F.col("t.account_id"),
        F.col("a.customer_id"),
        F.col("c.segment"),
        F.col("c.email"),
        F.date_sub(F.col("a.opened_date"), 56).alias("opened_date_minus_56d"),
    )
    .orderBy("opened_date_minus_56d", F.col("amount").desc())
)

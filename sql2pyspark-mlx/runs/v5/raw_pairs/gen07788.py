from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("amount").between(2200, 3100))
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .filter(F.col("tba.row_count") >= 7)
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("tba.amount_count"),
        F.col("tba.row_count"),
    )
    .orderBy(F.col("account_type").desc())
)

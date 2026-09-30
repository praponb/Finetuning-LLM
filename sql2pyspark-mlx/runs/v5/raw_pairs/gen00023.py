from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("amount") <= 3400)
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.countDistinct("amount").alias("unique_merchants"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("tba.amount_count"),
        F.col("tba.unique_merchants"),
    )
    .orderBy(F.col("amount_count").desc())
)

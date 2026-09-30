from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("amount") > 3900)
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
        F.max("amount").alias("max_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .filter(F.col("at.credit_count") > 23)
    .select(
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("at.credit_count"),
        F.col("at.max_amount"),
    )
)

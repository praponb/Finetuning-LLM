from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("merchant").isNull())
    .groupBy("account_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("at.amount_count"),
        F.col("at.starbucks_amount"),
    )
    .orderBy(F.col("customer_id").desc(), F.col("starbucks_amount").desc())
)

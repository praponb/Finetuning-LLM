from pyspark.sql import functions as F

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant").isin(["Shell", "Lazada"])), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        "account_type",
        F.when(F.col("balance") >= 72000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
    )
    .filter(F.col("savings_events") > 1)
    .orderBy(F.col("frozen_count").asc_nulls_first())
)

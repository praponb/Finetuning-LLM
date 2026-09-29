from pyspark.sql import functions as F

remaining_keys = (
    df_accounts
    .select("status")
    .intersect(
        df_accounts
        .filter(F.col("balance") >= 65000)
        .select("status")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("balance") >= 63000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
        F.avg(F.when(F.col("status") == "FROZEN", F.col("balance"))).alias("frozen_avg_balance"),
        F.min("balance").alias("min_balance"),
    )
    .filter(F.col("min_balance") > 0)
    .orderBy(F.col("min_balance").desc())
)

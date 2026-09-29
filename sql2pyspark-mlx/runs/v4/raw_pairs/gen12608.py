from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("balance_tier", F.when(F.col("balance") >= 82000, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["balance_tier", "account_type"], ["balance_tier"]],
        "balance_tier",
        "account_type",
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.count("*").alias("event_count"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "CHECKING", 1)), F.lit(0)), 2).alias("balance_per_checking"),
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
    )
)

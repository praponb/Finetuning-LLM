from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 88000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
        F.avg("balance").alias("avg_balance"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.sum("balance").alias("total_balance"),
        F.stddev_samp("balance").alias("balance_stddev"),
    )
    .orderBy(F.col("total_balance").desc())
)

from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 76000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("balance_per_frozen"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance"))).alias("checking_total_balance"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "CREDIT", 1)), F.lit(0)), 2).alias("balance_per_credit"),
    )
    .orderBy(F.col("balance_per_frozen").desc_nulls_last())
)

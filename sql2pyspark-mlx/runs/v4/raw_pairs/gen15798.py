from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "customer_id",
        F.when(F.col("balance") >= 84000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("balance_per_frozen"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
    )
    .orderBy(F.col("customer_id").asc_nulls_first())
)

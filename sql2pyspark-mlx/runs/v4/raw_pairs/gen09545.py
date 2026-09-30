from pyspark.sql import functions as F

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id").isin([20, 25, 28, 33])), "left_semi")
)

result = (
    kept
    .groupBy(
        F.date_trunc("year", "opened_date").alias("year_start"),
        F.when(F.col("balance") >= 85000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("num_status") >= 1)
)

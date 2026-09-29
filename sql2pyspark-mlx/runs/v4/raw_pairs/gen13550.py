from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("week_start", F.date_trunc("week", "opened_date"))
    .groupingSets([["status", "week_start"], ["status"], []], "status", "week_start")
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("balance_per_closed"),
        F.avg(F.when(F.col("account_type") == "SAVINGS", F.col("balance"))).alias("savings_avg_balance"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_rows"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.percentile_approx("balance", 0.95).alias("balance_p95"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("savings_avg_balance") < 0)
    .orderBy(F.col("balance_p95").desc())
)

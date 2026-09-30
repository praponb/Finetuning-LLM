from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("activity_month"))
    .agg(
        F.count_if(F.col("account_type") == "CREDIT").alias("credit_events"),
        F.avg(F.when(F.col("status") == "OPEN", F.col("balance"))).alias("open_avg_balance"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("balance_per_frozen"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("balance_per_closed"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_rows"),
    )
)

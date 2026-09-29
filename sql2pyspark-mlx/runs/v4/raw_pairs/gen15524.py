from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
    )
    .orderBy(F.col("savings_events").asc_nulls_first())
)

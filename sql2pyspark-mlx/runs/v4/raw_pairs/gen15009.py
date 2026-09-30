from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.date_trunc("YEAR", "txn_time").alias("year_start"),
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 0)
)

from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.date_trunc("WEEK", "txn_time").alias("week_start"))
    .agg(
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("amount_p90") < 1000000)
)

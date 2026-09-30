from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.date_trunc("YEAR", "start_date").alias("year_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.max(F.when(F.col("plan") == "basic", F.col("monthly_fee"))).alias("basic_max_monthly_fee"),
    )
    .filter(F.col("num_rows") > 1)
)

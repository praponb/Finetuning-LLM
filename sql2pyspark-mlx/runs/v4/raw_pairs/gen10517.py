from pyspark.sql import functions as F

result = (
    df_subscriptions
    .rollup("status", F.date_trunc("MONTH", "start_date").alias("start_month"))
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.max("end_date").alias("last_end_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
    .filter(F.col("stddev_monthly_fee") >= 26)
    .orderBy("status_values", "status")
)

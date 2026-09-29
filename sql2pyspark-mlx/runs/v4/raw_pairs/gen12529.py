from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([1, 2, 7]) & (F.col("status") == "ACTIVE") & (F.col("status") == "ACTIVE"))
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.min("monthly_fee").alias("min_monthly_fee"),
    )
    .filter(F.col("min_monthly_fee") >= 180)
    .drop("min_monthly_fee")
)

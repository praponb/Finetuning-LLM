from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.date_format("start_date", "yyyy-MM").alias("start_month"),
        "status",
        F.when(F.col("monthly_fee") >= 206, "high").when(F.col("monthly_fee") >= 104, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
    )
)

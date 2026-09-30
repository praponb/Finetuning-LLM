from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
    .orderBy("avg_monthly_fee")
)

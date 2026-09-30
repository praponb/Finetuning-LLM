from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_month"))
    .agg(
        F.countDistinct("monthly_fee").alias("unique_plans"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .orderBy("unique_plans", "end_month")
    .limit(50)
)

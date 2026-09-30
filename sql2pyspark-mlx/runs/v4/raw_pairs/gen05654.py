from pyspark.sql import functions as F

result = (
    df_subscriptions
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
        F.countDistinct("monthly_fee").alias("unique_plans"),
    )
    .orderBy(F.col("unique_plans").desc(), F.col("total_monthly_fee").desc())
)

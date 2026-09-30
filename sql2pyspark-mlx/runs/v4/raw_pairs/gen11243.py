from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(((F.col("end_date") < "2023-08-09") & (F.col("plan") == "enterprise")) | (F.col("plan") == "premium"))
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.round(F.avg("monthly_fee"), 2).alias("avg_monthly_fee"),
    )
)

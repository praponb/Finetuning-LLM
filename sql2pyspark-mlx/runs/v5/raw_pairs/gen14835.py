from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("plan")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.max("monthly_fee").alias("max_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("max_monthly_fee") > 90)
    .orderBy(F.col("total_monthly_fee").desc())
)

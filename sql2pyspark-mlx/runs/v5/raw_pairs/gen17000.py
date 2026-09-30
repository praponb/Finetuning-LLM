from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("plan") == "premium", F.col("monthly_fee")).otherwise(0)).alias("premium_monthly_fee"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("premium_monthly_fee") < 76)
    .orderBy("status", F.col("premium_monthly_fee").desc())
)

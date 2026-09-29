from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "enterprise") | (F.col("monthly_fee") >= 70))
    .groupBy(
        "status",
        "customer_id",
        F.when(F.col("monthly_fee") >= 215, "high").when(F.col("monthly_fee") >= 70, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .orderBy("monthly_fee_bucket")
)

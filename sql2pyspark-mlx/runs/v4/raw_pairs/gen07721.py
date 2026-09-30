from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 252, "high").when(F.col("monthly_fee") >= 120, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
        F.countDistinct("monthly_fee").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") < 4)
    .drop("unique_statuss")
)

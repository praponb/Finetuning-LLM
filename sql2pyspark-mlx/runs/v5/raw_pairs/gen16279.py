from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 255, "high").when(F.col("monthly_fee") >= 64, "mid").otherwise("low").alias("monthly_fee_bucket"),
        F.quarter("start_date").alias("start_quarter"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", 1).otherwise(0)).alias("paused_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("plan"))).alias("plan_values"),
    )
    .filter(F.col("paused_count") > 60)
    .select("monthly_fee_bucket", "start_quarter", "paused_count", "plan_values")
)

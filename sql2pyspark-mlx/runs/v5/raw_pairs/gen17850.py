from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 130) & (F.col("status") == "EXPIRED") & (F.col("status") == "PAUSED"))
    .withColumn("plan_status_key", F.concat_ws(",", "plan", "status"))
    .withColumn("days_since_start_date", F.datediff(F.current_date(), "start_date"))
)

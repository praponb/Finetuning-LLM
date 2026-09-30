from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .groupBy("plan", "status")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_customers"),
        F.max("end_date").alias("last_end_date"),
    )
    .filter(F.col("unique_customers") >= 6)
    .select("plan", "status", "unique_customers", "last_end_date")
    .orderBy(F.col("unique_customers").desc(), "plan", "status")
    .limit(10)
)

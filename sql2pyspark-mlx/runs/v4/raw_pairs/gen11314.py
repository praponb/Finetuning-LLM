from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", "plan")
    .agg(
        F.countDistinct("monthly_fee").alias("unique_customers"),
        F.min("end_date").alias("min_end_date"),
    )
    .filter(F.col("unique_customers") > 8)
    .select("status", "plan", "unique_customers", "min_end_date")
)

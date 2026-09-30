from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "ACTIVE") & (F.year("start_date") >= 2024) & (F.col("monthly_fee") <= 100))
    .withColumn("days_start_date_to_end_date", F.datediff("end_date", "start_date"))
    .withColumn("plan_length", F.length("plan"))
)

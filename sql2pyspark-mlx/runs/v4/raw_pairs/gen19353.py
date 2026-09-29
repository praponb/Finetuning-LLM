from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "CANCELLED") & (F.col("status") == "EXPIRED"))
    .withColumn("days_start_date_to_end_date", F.datediff("end_date", "start_date"))
)

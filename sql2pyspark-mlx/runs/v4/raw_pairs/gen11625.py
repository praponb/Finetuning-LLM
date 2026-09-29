from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .withColumn("plan_prefix", F.substring("plan", 1, 2))
    .withColumn("end_date_minus_81d", F.date_sub("end_date", 81))
)

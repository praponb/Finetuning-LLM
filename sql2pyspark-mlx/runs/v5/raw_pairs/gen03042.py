from pyspark.sql import functions as F

filtered_subscriptions = (
    df_subscriptions
    .filter((F.col("end_date") <= "2025-02-20") | (F.col("status") != "EXPIRED"))
    .withColumn("status_plan", F.concat("status", F.lit(" - "), "plan"))
)

agg = (
    filtered_subscriptions
    .groupBy("status")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)

result = agg

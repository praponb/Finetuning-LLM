from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(70, 150) & (F.col("customer_id") == 27))
    .select("id", "start_date", F.length("plan").alias("plan_length"))
    .orderBy("id", "start_date")
)

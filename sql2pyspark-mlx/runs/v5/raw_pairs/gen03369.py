from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2025-01-01", "2025-05-01"))
    .select("id", "start_date", "customer_id", F.length("plan").alias("plan_length"))
)

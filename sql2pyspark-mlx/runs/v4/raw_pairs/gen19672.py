from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((~F.col("plan").isin(["premium", "standard", "enterprise"]) & (F.col("end_date") > "2024-07-01")) | F.col("start_date").between("2023-10-01", "2025-04-01"))
    .withColumn("status_prefix", F.substring("status", 1, 2))
)

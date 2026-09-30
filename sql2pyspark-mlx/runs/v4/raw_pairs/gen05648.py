from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "basic"]) & (F.col("status") == "ACTIVE"))
    .select("plan", "status", "monthly_fee", F.length("status").alias("status_length"))
    .orderBy(F.col("status").desc())
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("status") == "ACTIVE") & (F.col("monthly_fee") > 120) & (F.col("plan") == "basic"))
    .select("id", "customer_id", F.length("status").alias("status_length"))
    .orderBy("status_length")
)

from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["standard", "enterprise", "basic"]))
    .withColumn("is_premium", F.when(F.col("plan") == "premium", 1).otherwise(0))
)

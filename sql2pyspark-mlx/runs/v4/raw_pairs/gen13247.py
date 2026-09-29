from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") != "standard") & (F.col("status") == "PAUSED") & F.col("customer_id").isin([17, 23, 24, 27]))
    .withColumn("is_active", F.when(F.col("status") == "ACTIVE", 1).otherwise(0))
)

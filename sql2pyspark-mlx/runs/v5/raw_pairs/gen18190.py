from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 2)
    .withColumn("is_standard", F.when(F.col("plan") == "standard", 1).otherwise(0))
    .orderBy("is_standard")
)

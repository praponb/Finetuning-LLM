from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("plan_length", F.length("plan"))
    .orderBy(F.col("monthly_fee").desc())
)

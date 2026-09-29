from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(((F.col("customer_id") == 16) & F.col("customer_id").isin([12, 18])) | (F.col("plan") == "standard"))
    .withColumn("clean_plan", F.trim("plan"))
    .withColumn("plan_prefix", F.substring("plan", 1, 2))
    .orderBy(F.col("status").desc(), "id")
    .limit(10)
)

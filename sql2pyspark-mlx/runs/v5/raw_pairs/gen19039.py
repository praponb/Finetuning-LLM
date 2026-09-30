from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & F.col("customer_id").isNotNull())
    .select("id", "start_date", "plan", F.substring("plan", 1, 4).alias("plan_prefix"))
    .orderBy("plan_prefix", F.col("id").desc())
)

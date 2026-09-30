from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & (F.col("end_date") < "2024-07-01"))
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0)).alias("standard_count"),
    )
    .filter(F.col("standard_count") < 55)
    .orderBy("plan", F.col("standard_count").desc())
    .limit(5)
)

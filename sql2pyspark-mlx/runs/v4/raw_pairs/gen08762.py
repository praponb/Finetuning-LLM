from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 20) & (F.col("plan") == "premium"))
    .groupBy("plan", "status")
    .agg(
        F.max("start_date").alias("last_start_date"),
        F.sum(F.when(F.col("plan") == "basic", 1).otherwise(0)).alias("basic_count"),
    )
    .filter(F.col("basic_count") < 33)
    .drop("basic_count")
    .orderBy("last_start_date")
)

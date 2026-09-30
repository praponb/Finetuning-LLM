from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Corporate")
    .groupBy("is_active")
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
    )
    .orderBy("latest_signup_date")
)

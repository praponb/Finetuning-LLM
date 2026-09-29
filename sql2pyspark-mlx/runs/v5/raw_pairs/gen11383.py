from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country", "is_active")
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.count("age").alias("age_count"),
    )
    .filter(F.col("age_count") > 3)
)

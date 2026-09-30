from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("email").isNotNull())
    .withColumn("age_scaled", F.round((F.col("age") * 100), 2))
    .withColumn("clean_city", F.trim("city"))
)

result = (
    filtered
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"), "country")
    .agg(
        F.max("age").alias("max_age"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
    )
    .filter(F.col("consumer_rows") >= 1)
)

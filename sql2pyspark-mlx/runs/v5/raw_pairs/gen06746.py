from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter((F.col("credit_limit") <= 14500) & F.col("name").like("%Mia%"))
    .withColumn("credit_limit_per_age", F.round((F.col("credit_limit") / F.col("age")), 2))
)

summary = (
    recent_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
)

result = summary.select("signup_month", "us_count", "segment_list")

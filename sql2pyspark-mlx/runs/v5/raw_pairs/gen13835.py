from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("age")).otherwise(0)).alias("consumer_age"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
    )
    .orderBy("quarter_start")
)

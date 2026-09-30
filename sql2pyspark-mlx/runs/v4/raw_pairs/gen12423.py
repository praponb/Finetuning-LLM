from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.date_format("signup_date", "yyyy-MM").alias("signup_month"),
        F.year("signup_date").alias("signup_year"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"),
        F.min("signup_date").alias("min_signup_date"),
    )
    .filter(F.col("us_count") < 38)
    .select("signup_month", "signup_year", "us_count", "min_signup_date")
)

from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(~F.col("country").isin(["DE", "SG", "US"]) & (F.col("credit_limit") < 15700))
)

customers_stats = (
    recent_customers
    .groupBy("city")
    .agg(
        F.count("age").alias("age_count"),
        F.min("signup_date").alias("min_signup_date"),
    )
)

result = (
    customers_stats
    .filter(F.col("age_count") >= 8)
    .select("city", "age_count", "min_signup_date")
)

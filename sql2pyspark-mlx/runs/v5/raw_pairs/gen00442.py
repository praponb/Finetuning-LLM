from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("city") == "Singapore")
    .withColumn("age_plus_credit_limit", (F.col("age") + F.col("credit_limit")))
)

is_active_summary = (
    base
    .groupBy("is_active")
    .agg(F.min("signup_date").alias("min_signup_date"))
)

result = is_active_summary

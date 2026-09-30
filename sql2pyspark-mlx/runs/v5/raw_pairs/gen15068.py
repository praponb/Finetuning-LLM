from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(~F.col("is_active"))
    .withColumn("clean_name", F.trim("name"))
)

summary = (
    recent_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.max("age").alias("max_age"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
)

result = (
    summary
    .filter(F.col("credit_limit_range") >= 44)
    .select("signup_quarter", "credit_limit_range")
)

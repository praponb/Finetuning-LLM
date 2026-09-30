from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("is_active") & (F.col("signup_date") < "2025-10-01"))

customers_stats = (
    filtered_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 12900, "high").when(F.col("credit_limit") >= 3000, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.min("signup_date").alias("min_signup_date"),
    )
)

result = customers_stats.orderBy("credit_limit_bucket")

from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
        F.when(F.col("credit_limit") >= 16700, "high").when(F.col("credit_limit") >= 6800, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"))
    .filter(F.col("tokyo_count") >= 27)
    .select("signup_month", "credit_limit_bucket", "tokyo_count")
    .orderBy("signup_month")
)

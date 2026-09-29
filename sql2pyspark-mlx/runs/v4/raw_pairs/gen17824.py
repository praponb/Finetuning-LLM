from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") > "2023-04-29")
    .groupBy(
        F.when(F.col("credit_limit") >= 13200, "high").when(F.col("credit_limit") >= 8200, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.count("credit_limit").alias("credit_limit_count"))
    .filter(F.col("credit_limit_count") >= 4)
    .orderBy("credit_limit_count", "credit_limit_bucket")
    .limit(3)
)

from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "country",
        F.when(F.col("credit_limit") >= 17900, "high").when(F.col("credit_limit") >= 4700, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.count("age").alias("age_count"),
        F.min("signup_date").alias("min_signup_date"),
    )
    .filter(F.col("age_count") >= 7)
)

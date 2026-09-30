from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 13400)
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.max("age").alias("max_age"),
        F.min("signup_date").alias("first_signup_date"),
        F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0)).alias("consumer_count"),
    )
    .filter(F.col("consumer_count") >= 42)
    .orderBy(F.col("first_signup_date").desc())
)

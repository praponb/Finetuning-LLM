from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") >= "2025-02-07")
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        "segment",
    )
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.max("age").alias("max_age"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
)

from pyspark.sql import functions as F

result = (
    df_customers
    .filter((~F.col("country").isin(["JP", "DE", "US"]) & F.month("signup_date").isin([5, 9, 12])) | F.col("is_active"))
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        F.month("signup_date").alias("signup_month"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
    .filter(F.col("consumer_count") > 3)
)

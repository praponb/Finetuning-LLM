from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("country").isin(["DE", "UK"]))
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", F.col("age")).otherwise(0)).alias("consumer_age"),
        F.min("credit_limit").alias("min_credit_limit"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
    )
    .filter(F.col("jp_count") > 5)
    .drop("jp_count")
    .orderBy(F.col("signup_cohort").desc())
    .limit(5)
)

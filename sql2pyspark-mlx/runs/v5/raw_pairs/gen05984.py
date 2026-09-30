from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.upper("email").like("%IAM%") & (F.col("segment") == "Corporate")) | (F.upper(F.trim("name")) == "GRACE KIM"))
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0)).alias("corporate_count"),
    )
    .filter(F.col("corporate_count") < 78)
    .orderBy(F.col("corporate_count").desc())
)

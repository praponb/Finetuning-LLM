from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") > 61)
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.count("age").alias("age_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("total_count").desc())
)

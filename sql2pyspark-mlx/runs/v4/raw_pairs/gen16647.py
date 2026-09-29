from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") != "JP")
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.count("age").alias("age_count"),
    )
    .filter(F.col("age_count") < 10)
    .select("signup_year", "first_signup_date", "age_count")
    .orderBy(F.col("signup_year").desc())
)

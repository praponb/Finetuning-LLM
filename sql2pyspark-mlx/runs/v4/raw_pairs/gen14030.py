from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") >= "2024-08-20")
    .groupBy(F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"))
    .pivot("city", ["Bangkok", "London", "New York", "Berlin"])
    .agg(F.count("age"))
    .orderBy("signup_cohort")
)

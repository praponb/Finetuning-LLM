from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter((F.col("credit_limit") <= 10700) | (F.col("age") < 40))
    .withColumn("is_uk", F.when(F.col("country") == "UK", 1).otherwise(0))
)

city_summary = (
    valid_customers
    .groupBy(
        "city",
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(F.round(F.stddev("is_uk"), 2).alias("stddev_is_uk"))
)

result = (
    city_summary
    .filter(F.col("stddev_is_uk") >= 52)
    .orderBy("city", "signup_cohort")
)

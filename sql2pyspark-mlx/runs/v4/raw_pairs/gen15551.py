from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        "country",
    )
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.countDistinct("credit_limit").alias("unique_countrys"),
    )
    .filter(F.col("unique_countrys") < 2)
    .drop("unique_countrys")
    .orderBy("signup_cohort")
)

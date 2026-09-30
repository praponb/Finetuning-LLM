from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") >= 15800)
    .cube(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        "country",
    )
    .agg(
        F.avg("age").alias("avg_age"),
        F.max("signup_date").alias("last_signup_date"),
    )
)

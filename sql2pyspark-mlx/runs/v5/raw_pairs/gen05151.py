from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNull())
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.avg("age").alias("avg_age"),
    )
    .orderBy("signup_cohort")
)

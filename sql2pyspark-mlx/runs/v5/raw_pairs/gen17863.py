from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 69, "high").when(F.col("age") >= 36, "mid").otherwise("low").alias("age_bucket"),
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
    )
    .agg(F.max("signup_date").alias("latest_signup_date"))
    .orderBy("age_bucket")
)

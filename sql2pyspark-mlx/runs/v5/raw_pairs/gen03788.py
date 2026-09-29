from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        (F.max("age") - F.min("age")).alias("age_range"),
        F.max("signup_date").alias("last_signup_date"),
        F.sum("age").alias("total_age"),
    )
    .filter(F.col("total_age") < 170)
    .drop("total_age")
    .orderBy("age_range", "is_active")
    .limit(50)
)

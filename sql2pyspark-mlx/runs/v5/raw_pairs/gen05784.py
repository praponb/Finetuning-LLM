from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", "segment")
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.max("signup_date").alias("last_signup_date"),
    )
    .orderBy(F.col("city").desc())
)

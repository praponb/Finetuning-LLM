from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        (F.max("age") - F.min("age")).alias("age_range"),
    )
    .filter(F.col("age_range") < 21)
    .orderBy(F.col("signup_quarter").desc())
)

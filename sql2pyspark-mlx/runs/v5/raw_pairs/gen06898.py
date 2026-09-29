from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isNotNull())
    .groupBy("segment", "city")
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
)

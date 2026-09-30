from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
)

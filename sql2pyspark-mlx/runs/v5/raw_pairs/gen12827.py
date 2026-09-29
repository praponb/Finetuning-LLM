from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "DE")
    .groupBy(F.year("signup_date").alias("signup_year"), "city")
    .agg(
        F.max("age").alias("max_age"),
        F.min("credit_limit").alias("min_credit_limit"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
)

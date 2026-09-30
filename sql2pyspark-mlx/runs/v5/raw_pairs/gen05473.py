from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.date_format("signup_date", "yyyy-MM").alias("signup_month"),
        "country",
        "city",
    )
    .agg(
        F.sum("age").alias("total_age"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
)

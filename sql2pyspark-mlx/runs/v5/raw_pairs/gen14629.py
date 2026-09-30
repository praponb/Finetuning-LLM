from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
)

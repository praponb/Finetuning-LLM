from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper("name").like("%IA %"))
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.max("age").alias("max_age"),
        F.sum(F.when(F.col("country") == "DE", F.col("credit_limit")).otherwise(0)).alias("de_credit_limit"),
    )
    .filter(F.col("max_age") >= 43)
    .orderBy(F.col("max_age").desc())
)

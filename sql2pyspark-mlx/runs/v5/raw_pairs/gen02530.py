from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.quarter("signup_date").alias("signup_quarter"),
        F.month("signup_date").alias("signup_month"),
    )
    .agg(F.count("age").alias("age_count"))
    .orderBy(F.col("signup_month").desc())
)

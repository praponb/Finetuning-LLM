from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") == "New York") & (F.col("segment") != "Consumer"))
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.max("age").alias("max_age"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
)

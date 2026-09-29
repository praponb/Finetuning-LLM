from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
    .orderBy(F.col("signup_month").desc(), F.col("stddev_age").desc())
)

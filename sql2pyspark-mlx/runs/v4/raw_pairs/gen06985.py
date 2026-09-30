from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") >= 12800) & F.col("age").between(31, 65))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.max("age").alias("max_age"),
        F.round(F.stddev("age"), 2).alias("stddev_age"),
    )
    .orderBy(F.col("max_age").desc())
)

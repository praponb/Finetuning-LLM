from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("credit_limit") < 13100)

customers_stats = (
    recent_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"), "city")
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
)

result = (
    customers_stats
    .filter(F.col("stddev_age") > 45)
    .select("signup_month", "stddev_age")
    .orderBy(F.col("stddev_age").desc(), F.col("signup_month").desc())
)

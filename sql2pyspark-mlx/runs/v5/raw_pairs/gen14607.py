from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("city") != "Berlin")

is_active_summary = (
    recent_customers
    .groupBy("is_active", F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
)

result = (
    is_active_summary
    .filter(F.col("stddev_age") < 30)
    .select("is_active", "stddev_age")
    .orderBy(F.col("stddev_age").desc(), "is_active")
)

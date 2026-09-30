from pyspark.sql import functions as F

recent_customers = df_customers.filter(F.col("email").like("%@mail.co.th"))

customers_stats = (
    recent_customers
    .groupBy("is_active")
    .agg(F.round(F.stddev("age"), 2).alias("stddev_age"))
)

result = customers_stats.select("is_active", "stddev_age")

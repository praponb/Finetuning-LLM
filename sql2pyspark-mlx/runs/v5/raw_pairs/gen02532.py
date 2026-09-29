from pyspark.sql import functions as F

valid_customers = df_customers.filter(F.col("email").isNull())

country_summary = (
    valid_customers
    .groupBy("country", "is_active")
    .agg(F.sum("age").alias("total_age"))
)

result = country_summary.orderBy("total_age")

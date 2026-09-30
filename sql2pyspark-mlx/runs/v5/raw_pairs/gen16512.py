from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.col("signup_date").alias("event_date"))
    .unionByName(df_products.select(F.col("launch_date").alias("event_date")))
    .distinct()
)

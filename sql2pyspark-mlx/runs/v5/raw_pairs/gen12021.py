from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") >= "2024-08-25")
    .groupBy("segment", "city")
    .agg(F.max("signup_date").alias("last_signup_date"))
    .orderBy(F.col("last_signup_date").desc())
)

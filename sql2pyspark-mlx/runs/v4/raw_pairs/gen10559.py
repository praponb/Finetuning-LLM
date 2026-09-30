from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .pivot("city", ["Berlin", "New York"])
    .agg(F.avg("age"))
)

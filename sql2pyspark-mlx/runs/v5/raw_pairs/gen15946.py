from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "London")
    .select("location", F.lit("department").alias("origin"))
    .union(df_customers.select("city", F.lit("customer").alias("origin")))
)

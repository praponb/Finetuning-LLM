from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(F.count("age").alias("age_count"))
    .filter(F.col("age_count") < 5)
    .orderBy(F.col("city").desc())
)

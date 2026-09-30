from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", "is_active")
    .agg(F.max("age").alias("max_age"))
    .filter(F.col("max_age") > 42)
    .orderBy("segment", F.col("max_age").desc())
)

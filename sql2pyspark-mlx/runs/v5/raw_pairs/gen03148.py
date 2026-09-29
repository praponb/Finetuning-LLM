from pyspark.sql import functions as F

result = (
    df_customers
    .agg(F.min("age").alias("min_age"))
    .orderBy(F.col("min_age").desc())
)

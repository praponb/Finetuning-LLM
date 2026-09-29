from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "DE")
    .agg(F.sum(F.when(F.col("country") == "DE", 1).otherwise(0)).alias("de_count"))
    .orderBy("de_count")
)

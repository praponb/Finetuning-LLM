from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") == "Berlin") & (F.col("age") != 38))
    .groupBy("country")
    .agg(F.min("age").alias("min_age"))
    .filter(F.col("min_age") > 32)
    .orderBy("country")
)

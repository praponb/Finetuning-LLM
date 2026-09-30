from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.min("sqft").alias("min_sqft"))
    .filter(F.col("min_sqft") >= 15500)
    .select("city", "min_sqft")
)

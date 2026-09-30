from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") < 15942)
    .agg(F.max("sqft").alias("max_sqft"))
    .orderBy("max_sqft")
)

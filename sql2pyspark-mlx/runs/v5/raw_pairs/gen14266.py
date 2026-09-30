from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 9704)
    .agg(F.sum("sqft").alias("total_sqft"))
)

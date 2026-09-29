from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.count("sqft").alias("sqft_count"))
    .orderBy(F.col("sqft_count").desc(), "city")
)

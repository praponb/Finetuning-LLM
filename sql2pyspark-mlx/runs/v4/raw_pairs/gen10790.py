from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
    .orderBy("region")
)

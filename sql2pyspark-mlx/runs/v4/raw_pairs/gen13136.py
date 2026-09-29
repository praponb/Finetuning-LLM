from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
    .filter(F.col("avg_sqft") >= 77)
)

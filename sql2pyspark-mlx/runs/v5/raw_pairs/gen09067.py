from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Airpor%"))
    .groupBy("region", F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
    .filter(F.col("stddev_sqft") > 76)
)

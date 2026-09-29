from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .pivot("region", ["Central", "North", "West", "South"])
    .agg(F.max("sqft"))
)

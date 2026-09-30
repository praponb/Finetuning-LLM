from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").isNull())
    .groupBy("city")
    .pivot("region", ["Central", "South", "North", "East"])
    .agg(F.sum("sqft"))
)

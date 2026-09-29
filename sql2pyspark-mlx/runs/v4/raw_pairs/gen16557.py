from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["Central", "East", "South"])
    .agg(F.sum("sqft"))
    .orderBy("city")
)

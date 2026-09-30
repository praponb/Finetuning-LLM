from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["South", "Central", "East"])
    .agg(F.max("sqft"))
    .orderBy("city")
)

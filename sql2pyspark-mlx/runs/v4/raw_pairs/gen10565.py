from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "New York")
    .groupBy("city")
    .pivot("region", ["South", "North", "Central", "East"])
    .agg(F.max("sqft"))
    .orderBy("city")
)

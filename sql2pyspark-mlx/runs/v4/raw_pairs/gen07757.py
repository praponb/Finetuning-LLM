from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["North", "East", "Central", "West"])
    .agg(F.count("sqft"))
    .orderBy("city")
)

from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["East", "Central", "West", "North"])
    .agg(F.sum("sqft"))
)

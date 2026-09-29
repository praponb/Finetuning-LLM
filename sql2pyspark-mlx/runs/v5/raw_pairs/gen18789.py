from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["East", "West", "North"])
    .agg(F.sum("sqft"))
    .orderBy("city")
)

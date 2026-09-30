from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["West", "North", "East"])
    .agg(F.sum("sqft"))
)

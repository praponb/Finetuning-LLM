from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 12795)
    .groupBy("region")
    .pivot("city", ["Chiang Mai", "London", "Singapore"])
    .agg(F.sum("sqft"))
    .orderBy("region")
)

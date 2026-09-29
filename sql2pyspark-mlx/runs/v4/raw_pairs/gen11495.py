from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Chiang Mai", "Berlin"])
    .agg(F.sum("sqft"))
)

from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["New York", "Berlin"])
    .agg(F.sum("sqft"))
    .orderBy("region")
)

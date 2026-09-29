from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Bangkok", "Berlin"])
    .agg(F.max("sqft"))
)

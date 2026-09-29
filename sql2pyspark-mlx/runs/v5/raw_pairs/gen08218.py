from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Bangkok", "London", "Tokyo", "Berlin"])
    .agg(F.avg("sqft"))
)

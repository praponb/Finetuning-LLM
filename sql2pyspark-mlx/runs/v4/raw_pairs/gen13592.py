from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["South", "North"]))
    .groupBy("region")
    .pivot("city", ["Singapore", "Chiang Mai"])
    .agg(F.count("sqft"))
    .orderBy("region")
)

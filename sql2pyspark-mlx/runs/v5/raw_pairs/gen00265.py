from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .pivot("city", ["Tokyo", "Bangkok", "Chiang Mai"])
    .agg(F.count("sqft"))
    .orderBy("region")
)

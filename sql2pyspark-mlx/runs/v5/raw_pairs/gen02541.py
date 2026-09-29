from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .pivot("region", ["Central", "South"])
    .agg(F.avg("sqft"))
    .select("city", F.col("Central").alias("central"), F.col("South").alias("south"))
)

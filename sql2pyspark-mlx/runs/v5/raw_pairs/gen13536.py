from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["East", "South", "Central"]))
    .agg(
        F.sum(F.when(F.col("region") == "West", 1).otherwise(0)).alias("west_count"),
        F.countDistinct("sqft").alias("unique_citys"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
    .orderBy("west_count")
)

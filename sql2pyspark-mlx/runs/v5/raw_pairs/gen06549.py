from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Chiang Mai")
    .groupBy("region")
    .agg(
        F.count(F.when(F.col("region") == "North", 1)).alias("north_count"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
    .filter(F.col("unique_regions") < 5)
    .orderBy(F.col("unique_regions").desc())
)

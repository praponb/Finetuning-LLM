from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
    .filter(F.col("north_sqft") >= 40)
    .orderBy(F.col("unique_regions").desc())
)

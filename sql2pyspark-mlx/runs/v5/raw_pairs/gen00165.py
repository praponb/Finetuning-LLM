from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region").isin(["East", "North"]) & (F.col("sqft") <= 5946)) | F.col("sqft").between(7682, 12694))
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("sqft")).otherwise(0)).alias("tokyo_sqft"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .orderBy(F.col("region").desc())
    .limit(10)
)

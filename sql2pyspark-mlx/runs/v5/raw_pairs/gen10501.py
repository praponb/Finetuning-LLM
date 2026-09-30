from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.when(F.col("sqft") >= 15121, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.sum(F.when(F.col("region") == "Central", F.col("sqft")).otherwise(0)).alias("central_sqft"),
    )
    .filter(F.col("cnt") < 1000)
)

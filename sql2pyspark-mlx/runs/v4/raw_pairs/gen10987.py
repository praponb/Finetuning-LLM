from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(((F.length("store_name") > 6) & F.col("city").isin(["New York", "Chiang Mai", "London"])) | (F.col("sqft") <= 13633))
    .withColumn("region_length", F.length("region"))
)

stores_stats = (
    recent_stores
    .groupBy(
        F.when(F.col("region").isin(["Central", "North"]), "tier_1").otherwise("other").alias("region_group"),
    )
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("region_length")).otherwise(0)).alias("north_region_length"),
    )
)

result = (
    stores_stats
    .select("region_group", "north_region_length")
    .orderBy("region_group")
)

from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("store_name").isNotNull())

stores_stats = (
    valid_stores
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
    )
)

result = (
    stores_stats
    .select("region", "north_sqft")
    .orderBy(F.col("region").desc())
)

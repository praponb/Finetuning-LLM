from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("store_name").like("%Rive%"))
    .select(
        "*",
        F.substring("region", 1, 3).alias("region_prefix"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

result = (
    cleaned
    .groupBy("region")
    .pivot("city", ["Berlin", "Tokyo"])
    .agg(F.sum("sqft").alias("total"))
)

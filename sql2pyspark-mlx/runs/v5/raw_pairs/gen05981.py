from pyspark.sql import functions as F

recent_stores = df_stores.filter(F.col("store_name").like("Ai%"))

stores_stats = (
    recent_stores
    .groupBy("region")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.round(((100 * F.sum(F.when(F.col("region") == "East", 1).otherwise(0))) / F.count("*")), 1).alias("pct_east"),
    )
)

result = (
    stores_stats
    .filter(F.col("pct_east") < 58)
    .select("region", "max_sqft", "pct_east")
    .orderBy(F.col("max_sqft").desc(), "region")
    .limit(20)
)

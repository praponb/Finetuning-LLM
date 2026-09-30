from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("city") == "Chiang Mai"))
    .groupBy("region")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("store_count"),
    )
    .orderBy(F.col("store_count").desc(), "region")
    .limit(5)
)

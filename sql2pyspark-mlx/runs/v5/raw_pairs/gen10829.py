from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") != "Central") | ~F.col("store_name").like("%Downtow%"))
    .groupBy("city", "region")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("sqft").alias("sqft_count"),
    )
    .orderBy(F.col("sqft_count").desc())
)

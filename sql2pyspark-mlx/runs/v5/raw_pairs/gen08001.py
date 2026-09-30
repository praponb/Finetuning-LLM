from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(~(F.col("sqft").between(6787, 11121) & F.col("store_name").isNull()))
)

city_summary = (
    recent_stores
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft")).otherwise(0)).alias("berlin_sqft"),
    )
)

result = (
    city_summary
    .filter(F.col("berlin_sqft") >= 31)
    .select("city", "region_values")
)

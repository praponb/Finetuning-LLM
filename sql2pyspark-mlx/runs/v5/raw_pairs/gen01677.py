from pyspark.sql import functions as F

valid_stores = df_stores.filter((F.col("city") != "Tokyo") & (F.col("sqft") <= 5436))

region_summary = (
    valid_stores
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft")).otherwise(0)).alias("berlin_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
)

result = (
    region_summary
    .filter(F.col("berlin_sqft") < 57)
    .select("region", "berlin_sqft", "region_list")
)

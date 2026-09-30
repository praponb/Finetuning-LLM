from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.countDistinct("city").alias("distinct_cities_2"),
        F.percentile_approx("sqft", 0.5).alias("sqft_p50"),
    )
)

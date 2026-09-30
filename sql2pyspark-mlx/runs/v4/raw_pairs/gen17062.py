from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
        F.countDistinct("city").alias("distinct_cities"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .orderBy(F.col("chiang_mai_count").desc_nulls_first())
)

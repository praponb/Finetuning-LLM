from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city", "region").orderBy("sqft", "id")

metrics = (
    df_stores
    .withColumn("sqft_bucket", F.ntile(4).over(w))
    .withColumn("sqft_bucket_2", F.ntile(4).over(w2))
)

result = (
    metrics
    .withColumn("sqft_bucket_tier", F.when(F.col("sqft_bucket") >= 8, "HIGH").otherwise("LOW"))
    .rollup("sqft_bucket_tier")
    .agg(
        F.count_if(F.col("region") == "South").alias("south_events"),
        F.countDistinct("region").alias("distinct_regions"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("sqft_bucket_2")).otherwise(0)).alias("tokyo_sqft_bucket_2"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.grouping("sqft_bucket_tier").alias("grouping_level"),
    )
    .orderBy(F.col("distinct_regions").desc_nulls_first())
)

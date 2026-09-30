from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("east_count").desc())

base = (
    df_stores
    .filter(F.col("sqft") != 6847)
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        F.concat("store_name", F.lit(" - "), "region").alias("store_name_region"),
    )
)

result = (
    base
    .groupBy(
        "region",
        F.when(F.col("sqft") >= 15256, "HIGH").otherwise("LOW").alias("sqft_tier"),
    )
    .agg(
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .filter(F.col("east_count") >= 1)
    .withColumn("east_count_percent_rank", F.percent_rank().over(w))
)

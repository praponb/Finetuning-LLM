from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("region_length").desc())
w2 = Window.orderBy(F.col("sqft").asc_nulls_first())
w3 = Window.orderBy("sqft")
w4 = Window.partitionBy("region")
w5 = Window.partitionBy("city").orderBy(F.col("region_length").desc_nulls_first())
w6 = Window.partitionBy("city")
w7 = Window.partitionBy("city").orderBy(F.col("sqft_dense_rank").asc_nulls_last())
w8 = Window.partitionBy("city", "region")
w9 = Window.partitionBy("city", "region").orderBy(F.col("region_length_cume_dist").desc_nulls_last())

valid = (
    df_stores
    .filter(F.col("region") == "East")
    .withColumn("clean_city", F.trim("city"))
    .withColumn("store_name_region", F.concat("store_name", F.lit(", "), "region"))
)

matched_keys = (
    valid
    .select("region")
    .exceptAll(
        df_stores
        .filter(F.col("opened_date") < "2024-12-01")
        .select("region")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("region").isNotNull())
    .select("*", F.length("region").alias("region_length"))
)

metrics = (
    base
    .withColumn("region_length_rank", F.rank().over(w))
    .withColumn("sqft_dense_rank", F.dense_rank().over(w2))
    .withColumn("sqft_rank", F.rank().over(w3))
    .withColumn("region_min_region_length", F.min("region_length").over(w4))
    .withColumn("region_length_cume_dist", F.cume_dist().over(w5))
)

result = (
    metrics
    .select(
        "*",
        F.max("region_length_rank").over(w6).alias("city_max_region_length_rank"),
        F.round((F.col("sqft") / F.sum("sqft").over(w6)), 4).alias("sqft_share"),
        F.cume_dist().over(w7).alias("sqft_dense_rank_cume_dist"),
        F.count("region_length_rank").over(w8).alias("city_count_region_length_rank"),
        F.percent_rank().over(w9).alias("region_length_cume_dist_percent_rank"),
    )
)

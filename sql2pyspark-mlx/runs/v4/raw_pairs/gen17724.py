from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.length("store_name").alias("store_name_length"),
        F.regexp_replace("store_name", " ", "_").alias("store_name_slug"),
    )
)

result = (
    filtered
    .withColumn("store_name_length_tier", F.when(F.col("store_name_length") >= 68, "PREMIUM").otherwise("STANDARD"))
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 14907, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["store_name_length_tier", "sqft_tier"], ["store_name_length_tier"]],
        "store_name_length_tier",
        "sqft_tier",
    )
    .agg(
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.bit_or("sqft").alias("sqft_bit_or_2"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("city").alias("distinct_cities"),
        F.avg("store_name_length").alias("avg_store_name_length"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("sqft_bit_or") >= 1)
)

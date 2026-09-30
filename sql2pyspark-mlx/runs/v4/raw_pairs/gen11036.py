from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 17087, "LARGE").otherwise("SMALL"))
    .withColumn("year_start", F.date_trunc("YEAR", "opened_date"))
    .rollup("sqft_tier", "year_start", "city")
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.grouping("sqft_tier").alias("grouping_id"),
    )
)

from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter(F.col("opened_date").between("2023-12-01", "2024-12-07"))
    .select(
        "*",
        F.when(F.col("city") == "Chiang Mai", 1).otherwise(0).alias("is_chiang_mai"),
        F.upper("region").alias("region_upper"),
    )
)

result = (
    filtered
    .groupBy("city")
    .pivot("region", ["West", "South"])
    .agg(
        F.sum("is_chiang_mai").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "city",
        F.col("West_value_sum").alias("west_value_sum"),
        F.col("West_item_count").alias("west_item_count"),
        F.col("South_value_sum").alias("south_value_sum"),
        F.col("South_item_count").alias("south_item_count"),
    )
)

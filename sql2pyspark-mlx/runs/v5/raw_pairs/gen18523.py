from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.col("region").isin(["West", "Central"]) & ((F.col("city") == "Bangkok") | ~F.col("city").isin(["Tokyo", "Bangkok", "New York"])))
)

agg = (
    filtered_stores
    .groupBy(
        F.when(F.col("region").isin(["South", "West"]), "group_a").otherwise("other").alias("region_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
    )
)

result = (
    agg
    .filter(F.col("west_sqft") < 53)
    .select("region_group", "pct_west", "west_sqft")
)

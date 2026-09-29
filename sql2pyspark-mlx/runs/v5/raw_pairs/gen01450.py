from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("region").isin(["Central", "North"]), "tier_1").otherwise("other").alias("region_group"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
    )
    .filter(F.col("new_york_count") > 59)
    .select("region_group", "new_york_count")
    .orderBy("region_group", F.col("new_york_count").desc())
    .limit(5)
)

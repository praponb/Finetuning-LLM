from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["East", "North", "West"]))
    .groupBy(
        "city",
        F.when(F.col("region").isin(["West", "South"]), "group_a").otherwise("other").alias("region_group"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") > 31)
    .select("city", "region_group", "city_list", "avg_sqft")
    .orderBy("region_group")
)

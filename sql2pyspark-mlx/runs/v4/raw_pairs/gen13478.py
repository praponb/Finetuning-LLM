from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "city",
        F.when(F.col("city").isin(["London", "Tokyo"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .filter(F.col("avg_sqft") >= 20)
    .select("city", "city_group", "avg_sqft", "region_list")
)

from pyspark.sql import functions as F

filtered_stores = df_stores.filter((F.col("sqft") == 11687) & (F.col("sqft") == 15505))

summary = (
    filtered_stores
    .groupBy(
        F.when(F.col("city").isin(["Singapore", "London"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
    )
)

result = summary.select("city_group", "pct_west")

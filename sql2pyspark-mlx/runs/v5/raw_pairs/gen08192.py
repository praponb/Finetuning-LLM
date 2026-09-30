from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter((F.col("region") == "West") & F.col("region").isin(["West", "Central"]))
    .withColumn("city_title", F.initcap(F.trim("city")))
)

city_summary = (
    filtered_stores
    .groupBy("city")
    .agg(F.min("sqft").alias("min_sqft"))
)

result = (
    city_summary
    .filter(F.col("min_sqft") < 13300)
    .select("city", "min_sqft")
)

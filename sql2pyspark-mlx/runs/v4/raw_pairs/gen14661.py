from pyspark.sql import functions as F

base = (
    df_stores
    .filter(~F.col("region").isin(["South", "Central"]) & (F.length("store_name") >= 7) & (F.col("region") == "East"))
)

city_summary = (
    base
    .groupBy("city")
    .agg(F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"))
)

result = (
    city_summary
    .filter(F.col("new_york_count") > 9)
    .select("city", "new_york_count")
    .orderBy(F.col("new_york_count").desc())
)

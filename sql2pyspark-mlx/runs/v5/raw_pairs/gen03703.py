from pyspark.sql import functions as F

base = df_stores.filter(F.col("city") == "Berlin")

stores_stats = (
    base
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft")).otherwise(0)).alias("bangkok_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = (
    stores_stats
    .filter(F.col("bangkok_sqft") > 57)
    .select("opened_month", "bangkok_sqft", "city_list")
    .orderBy("bangkok_sqft", F.col("opened_month").desc())
)

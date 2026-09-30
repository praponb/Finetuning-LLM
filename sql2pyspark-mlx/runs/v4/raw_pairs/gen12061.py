from pyspark.sql import functions as F

filtered_stores = df_stores.filter(F.col("region") == "North")

stores_stats = (
    filtered_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.avg("sqft").alias("avg_sqft"))
)

result = (
    stores_stats
    .filter(F.col("avg_sqft") < 13800)
    .select("opened_year", "avg_sqft")
)

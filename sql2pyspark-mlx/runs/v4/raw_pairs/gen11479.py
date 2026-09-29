from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([5, 6, 11]) & (F.col("store_name").like("%Old%") | (F.col("region") != "North")))
    .groupBy("region", "city")
    .agg(F.avg("sqft").alias("avg_sqft"))
    .filter(F.col("avg_sqft") > 10300)
    .orderBy("city", F.col("avg_sqft").desc())
)

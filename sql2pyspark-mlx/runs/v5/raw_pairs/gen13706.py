from pyspark.sql import functions as F

src = (
    df_stores
    .filter(F.col("opened_date") < "2025-09-08")
    .withColumn("city_lower", F.lower("city"))
)

stores_stats = (
    src
    .groupBy(F.month("opened_date").alias("opened_month"), "city")
    .agg(F.max("sqft").alias("max_sqft"))
)

result = (
    stores_stats
    .filter(F.col("max_sqft") > 9200)
    .orderBy(F.col("max_sqft").desc())
)

from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "London", "New York"]))
    .withColumn("country_city_key", F.concat_ws(",", "country", "city"))
)

city_summary = (
    filtered_suppliers
    .groupBy("city")
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
)

result = (
    city_summary
    .select("city", "avg_rating")
    .orderBy(F.col("avg_rating").desc(), "city")
)

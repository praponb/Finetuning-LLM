from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%OKY%"))
    .withColumn("city_prefix", F.substring("city", 1, 3))
)

summary = (
    base
    .groupBy("country", "city")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.min("rating").alias("min_rating"),
    )
)

result = summary.orderBy("min_rating")

from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter((F.col("rating") < 4.2) | (F.col("rating") < 2.2))

summary = (
    recent_suppliers
    .groupBy("city")
    .agg(F.min("rating").alias("min_rating"))
)

result = (
    summary
    .select("city", "min_rating")
    .orderBy(F.col("min_rating").desc(), "city")
    .limit(10)
)

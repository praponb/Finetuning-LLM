from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .groupBy("city")
    .agg(F.min("rating").alias("min_rating"))
    .filter(F.col("min_rating") >= 2.4)
    .select("city", "min_rating")
    .orderBy(F.col("min_rating").desc())
)

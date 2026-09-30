from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg((F.max("rating") - F.min("rating")).alias("rating_range"))
    .filter(F.col("rating_range") >= 29)
    .select("city", "rating_range")
    .orderBy(F.col("rating_range").desc())
)

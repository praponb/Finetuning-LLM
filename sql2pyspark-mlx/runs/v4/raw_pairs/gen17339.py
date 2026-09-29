from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%oky%"))
    .groupBy("city", "country")
    .agg(F.count("rating").alias("rating_count"))
    .filter(F.col("rating_count") >= 8)
    .orderBy(F.col("country").desc())
)

from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 2.1) & F.lower("supplier_name").like("%uro%") & (F.col("city") == "New York"))
    .groupBy("country")
    .agg(
        F.max("rating").alias("max_rating"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
    .orderBy(F.col("max_rating").desc(), F.col("pct_chiang_mai").desc())
)

from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.length("supplier_name") < 13)
    .withColumn("city_prefix", F.substring("city", 1, 2))
)

summary = (
    src
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"))
)

result = summary.orderBy(F.col("rating_bucket").desc())

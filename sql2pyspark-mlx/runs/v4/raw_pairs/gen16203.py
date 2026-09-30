from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("supplier_name").isNotNull() & (F.col("country") == "SG"))

suppliers_stats = (
    recent_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
    )
)

result = (
    suppliers_stats
    .select("rating_bucket", "stddev_rating", "bangkok_count")
    .orderBy("stddev_rating")
)

from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("supplier_name").like("Pac%"))
    .withColumn("is_tokyo", F.when(F.col("city") == "Tokyo", 1).otherwise(0))
)

suppliers_stats = (
    src
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.count("is_tokyo").alias("is_tokyo_count"),
    )
)

result = (
    suppliers_stats
    .orderBy("pct_london", "city")
    .limit(50)
)

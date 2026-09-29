from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 4.4, "high").when(F.col("s.rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count(F.when(F.col("p.brand") == "Initech", 1)).alias("initech_count"),
        F.min(F.col("p.launch_date")).alias("min_launch_date"),
    )
    .orderBy("rating_bucket", F.col("min_launch_date").desc())
    .limit(10)
)

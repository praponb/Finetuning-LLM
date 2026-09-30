from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 3.7, "high").when(F.col("s.rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        F.when(F.col("s.country").isin(["JP", "TH"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(
        F.min(F.col("p.launch_date")).alias("min_launch_date"),
        F.count(F.when(F.col("p.category") == "Sports", 1)).alias("sports_count"),
    )
    .orderBy(F.col("rating_bucket").desc())
)

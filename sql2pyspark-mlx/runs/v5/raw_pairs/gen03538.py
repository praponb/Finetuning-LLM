from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 4.5, "high").when(F.col("s.rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.min(F.col("p.price")).alias("min_price"),
        F.max(F.col("p.launch_date")).alias("latest_launch_date"),
    )
    .orderBy("rating_bucket")
)

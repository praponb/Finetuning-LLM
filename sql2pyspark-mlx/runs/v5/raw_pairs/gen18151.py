from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 3.4, "high").when(F.col("s.rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max(F.col("p.launch_date")).alias("latest_launch_date"))
)

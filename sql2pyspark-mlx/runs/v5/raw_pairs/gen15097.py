from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(
        F.col("s.country"),
        F.when(F.col("s.rating") >= 3.4, "high").when(F.col("s.rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
)

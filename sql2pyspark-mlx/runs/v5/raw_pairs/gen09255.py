from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .groupBy(
        F.when(F.col("s.rating") >= 4.6, "high").when(F.col("s.rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.col("ss.units")).alias("total_units"),
        F.max(F.col("ss.units")).alias("max_units"),
    )
    .orderBy(F.col("total_units").desc(), "rating_bucket")
    .limit(3)
)

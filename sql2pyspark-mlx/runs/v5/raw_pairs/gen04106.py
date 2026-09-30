from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.rating") >= 3.9, "high").when(F.col("s.rating") >= 2.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.col("ss.units")).alias("total_units"),
    )
    .orderBy(F.col("row_count").desc())
)

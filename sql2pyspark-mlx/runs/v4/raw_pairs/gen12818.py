from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.supplier_id").isin([14, 31, 33]))
    .groupBy(
        F.when(F.col("s.rating") >= 3.8, "high").when(F.col("s.rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sum(F.col("p.stock_qty")).alias("total_stock_qty"))
    .orderBy(F.col("total_stock_qty").desc())
)

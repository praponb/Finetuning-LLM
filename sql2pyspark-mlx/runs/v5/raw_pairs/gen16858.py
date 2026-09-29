from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(
        F.when(F.col("s.rating") >= 3.6, "high").when(F.col("s.rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min(F.col("p.stock_qty")).alias("min_stock_qty"))
)

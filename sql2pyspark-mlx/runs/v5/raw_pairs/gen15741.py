from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("stock_qty").alias("unique_suppliers"),
        F.min("price").alias("min_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.country"),
        F.col("t.unique_suppliers"),
        F.col("t.min_price"),
    )
)

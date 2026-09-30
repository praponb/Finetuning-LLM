from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("product_name").like("Wat%"))
    .groupBy("supplier_id")
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(F.col("s.city"), F.col("ss.stock_qty_range"), F.col("ss.avg_cost"))
)

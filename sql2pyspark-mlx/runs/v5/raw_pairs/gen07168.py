from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count("stock_qty").alias("stock_qty_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("t.stock_qty_count"))
    .orderBy(F.col("id").desc())
    .limit(5)
)

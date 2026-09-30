from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.size("tags") >= 1)
    .groupBy("supplier_id")
    .agg(F.max("stock_qty").alias("max_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.max_stock_qty") >= 400)
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("t.max_stock_qty"))
)

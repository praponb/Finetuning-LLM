from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(F.min("stock_qty").alias("min_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .filter(F.col("S.min_stock_qty") > 310)
    .select(F.col("s.id"), F.col("s.rating"), F.col("S.min_stock_qty"))
    .orderBy(F.col("rating").desc(), "id")
    .limit(5)
)

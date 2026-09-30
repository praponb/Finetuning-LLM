from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("supplier_id") == 9)
    .groupBy("supplier_id")
    .agg((F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.stock_qty_range") >= 39)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.rating"),
        F.col("ss.stock_qty_range"),
    )
)

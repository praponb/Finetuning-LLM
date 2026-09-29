from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.pct_grocery") >= 77)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("t.pct_grocery"),
        F.col("t.stock_qty_range"),
    )
)

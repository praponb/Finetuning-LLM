from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("price").between(660, 1100))
    .groupBy("supplier_id")
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(F.col("s.country"), F.col("ss.stock_qty_range"), F.col("ss.min_stock_qty"))
)

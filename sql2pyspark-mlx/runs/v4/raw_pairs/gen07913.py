from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "bestseller"))
    .groupBy(F.col("s.country"))
    .agg(
        (F.max(F.col("p.stock_qty")) - F.min(F.col("p.stock_qty"))).alias("stock_qty_range"),
        F.max(F.col("p.launch_date")).alias("last_launch_date"),
    )
    .orderBy(F.col("country").desc())
    .limit(10)
)

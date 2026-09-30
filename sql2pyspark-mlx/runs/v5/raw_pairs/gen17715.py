from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(~F.col("s.country").isin(["US", "DE"]))
    .groupBy(F.col("s.id"), F.col("s.supplier_name"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.min(F.col("p.stock_qty")).alias("min_stock_qty"),
        (F.max(F.col("p.stock_qty")) - F.min(F.col("p.stock_qty"))).alias("stock_qty_range"),
    )
    .orderBy("product_count")
)

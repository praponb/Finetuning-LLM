from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.round(F.stddev(F.col("p.cost")), 2).alias("stddev_cost"),
        (F.max(F.col("p.stock_qty")) - F.min(F.col("p.stock_qty"))).alias("stock_qty_range"),
    )
)

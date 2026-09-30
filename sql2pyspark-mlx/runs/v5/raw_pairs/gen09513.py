from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "clearance"))
    .groupBy(F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.sum(F.col("p.cost")).alias("total_cost"),
        F.sum(F.col("p.stock_qty")).alias("total_stock_qty"),
    )
    .filter(F.col("total_stock_qty") > 800)
    .orderBy(F.col("product_count").desc(), F.col("country").desc())
)

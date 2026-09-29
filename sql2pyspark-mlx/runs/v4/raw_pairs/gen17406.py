from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("cost") >= 530)
    .groupBy("supplier_id")
    .agg(F.count("stock_qty").alias("stock_qty_count"))
)

result = (
    df_suppliers.alias("s")
    .join(products_by_supplier.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.stock_qty_count") > 8)
    .select(F.col("s.city"), F.col("s.country"), F.col("t.stock_qty_count"))
)

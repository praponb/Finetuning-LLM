from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("price").isNull())
    .groupBy("supplier_id")
    .agg(F.sum("stock_qty").alias("total_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.country"), F.col("t.total_stock_qty"))
    .orderBy(F.col("country").desc(), "id")
)

from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("supplier_id").isin([2, 27]))
    .groupBy("supplier_id")
    .agg(F.countDistinct("stock_qty").alias("unique_brands"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("t.unique_brands"))
    .orderBy("supplier_name")
)

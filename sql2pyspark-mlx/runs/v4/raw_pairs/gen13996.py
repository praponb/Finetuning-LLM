from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "eco"))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("s.country"),
    )
    .orderBy(F.col("product_id").desc(), "id")
    .limit(10)
)

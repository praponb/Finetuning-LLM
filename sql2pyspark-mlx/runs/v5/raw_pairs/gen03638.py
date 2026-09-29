from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "clearance") & F.col("p.brand").isin(["Acme", "Initech"]))
    .select(
        F.col("i.quantity"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("s.city"),
        F.col("s.rating"),
    )
    .orderBy(F.col("rating").desc())
)

from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter((F.col("s.country") != "US") & F.lower(F.col("s.supplier_name")).like("%aci%"))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("s.supplier_name"),
    )
    .orderBy("supplier_id")
)

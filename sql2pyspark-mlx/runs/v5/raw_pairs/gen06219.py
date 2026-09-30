from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("i.last_restock") < "2025-08-24") & F.col("s.city").isin(["Berlin", "Singapore"]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("p.cost"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
)

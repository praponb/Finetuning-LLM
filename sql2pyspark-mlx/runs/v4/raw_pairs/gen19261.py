from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("s.city") == "Singapore") & F.col("s.city").isNotNull())
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.supplier_id"),
        F.col("s.city"),
        F.col("s.rating"),
    )
)

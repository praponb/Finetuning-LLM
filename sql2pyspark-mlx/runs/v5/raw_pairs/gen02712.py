from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .filter(F.col("s.rating") < 2.7)
    .select(
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.col("s.rating"),
        F.col("s.city"),
    )
    .orderBy("rating")
)

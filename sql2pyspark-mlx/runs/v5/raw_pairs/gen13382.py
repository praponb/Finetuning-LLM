from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .filter(F.col("i.last_restock").between("2023-04-01", "2023-05-01"))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.region"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
    )
    .orderBy("supplier_id", "last_restock")
)

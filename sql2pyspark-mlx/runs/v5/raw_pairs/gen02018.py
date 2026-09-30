from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter((F.col("p.stock_qty") < 132) & F.col("p.brand").isin(["Stark", "Acme"]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("p.brand"),
        F.substring(F.col("p.category"), 1, 3).alias("category_prefix"),
    )
)

from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("i.product_id").isin([3, 10]) & F.col("i.store_id").isin([1, 3, 4]))
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("p.brand"),
        F.col("p.price"),
        F.col("s.rating"),
        F.when(F.col("i.quantity") >= 198, "High").when(F.col("i.quantity") >= 100, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

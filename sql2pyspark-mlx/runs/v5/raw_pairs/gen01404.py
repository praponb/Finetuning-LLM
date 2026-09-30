from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.cost").isNotNull())
    .select(
        F.col("i.quantity"),
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.col("p.category"),
    )
)

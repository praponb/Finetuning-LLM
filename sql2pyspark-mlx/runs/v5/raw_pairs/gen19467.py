from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(~((F.col("p.stock_qty") <= 289) & F.col("i.product_id").isin([2, 4, 8])))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
    )
    .orderBy("product_name", F.col("id").desc())
)

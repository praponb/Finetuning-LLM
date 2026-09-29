from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.product_id").isin([4, 8]) & F.col("i.store_id").isin([1, 4, 5, 6]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("p.price"),
    )
    .orderBy(F.col("supplier_id").desc(), F.col("price").desc(), "id")
    .limit(20)
)

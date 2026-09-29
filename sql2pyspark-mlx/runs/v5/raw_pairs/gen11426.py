from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.col("supplier_id") == 15).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
    )
    .orderBy("last_restock")
)

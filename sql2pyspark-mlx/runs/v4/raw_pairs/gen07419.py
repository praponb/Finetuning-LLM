from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.size("tags") >= 2).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .filter(F.col("i.product_id").isin([7, 9]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
    )
)

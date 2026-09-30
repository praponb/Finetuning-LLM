from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.product_id").isin([4, 9]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("p.category"),
        F.col("p.stock_qty"),
    )
    .orderBy("last_restock", "category")
)

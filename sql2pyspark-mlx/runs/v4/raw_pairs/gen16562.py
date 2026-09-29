from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter((F.length(F.col("p.product_name")) >= 9) & (F.col("p.price") >= 1120))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("p.cost"),
    )
    .orderBy("quantity")
)

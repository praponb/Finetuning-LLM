from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.price") >= 320)
    .select(
        F.col("p.cost"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
    )
    .orderBy("store_id", F.col("cost").desc())
)

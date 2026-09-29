from pyspark.sql import functions as F

enriched = (
    df_inventory
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.quantity",
        "e.last_restock",
        F.col("d.price"),
    )
)

result = enriched.select("id", "store_id", "product_id", "quantity", "last_restock", "price")

from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-09-28").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.quantity_rounded",
        F.col("d.price"),
    )
)

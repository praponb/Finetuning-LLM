from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.array_contains("tags", "premium"))
    .select(
        "*",
        (F.col("price") * F.col("stock_qty")).alias("price_times_stock_qty"),
        F.initcap(F.trim("category")).alias("category_title"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") < "2024-09-09"), "left_semi")
)

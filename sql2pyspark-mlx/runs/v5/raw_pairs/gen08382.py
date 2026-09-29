from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id")

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-09-02").cast("date")) & (F.col("brand") == "Initech"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.floor("price").alias("price_whole"),
        F.sort_array("tags").alias("sorted_tags"),
    )
)

matched_keys = (
    base
    .select("id")
    .subtract(
        df_products
        .filter(F.array_contains("tags", "new"))
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("launch_date") >= F.lit("2023-07-15").cast("date"))
    .select(
        "*",
        F.round("stock_qty", 0).alias("stock_qty_rounded"),
        F.col("stock_qty").cast("string").alias("stock_qty_str"),
    )
)

enriched = (
    valid
    .select(
        "brand",
        "price",
        "cost",
        "stock_qty_rounded",
        F.min("stock_qty_rounded").over(w).alias("category_min_stock_qty_rounded"),
    )
)

base_products = (
    enriched
    .filter(F.col("cost") < 570)
    .select(
        "brand",
        "price",
        "cost",
        "stock_qty_rounded",
        "category_min_stock_qty_rounded",
        F.lower("brand").alias("brand_lower"),
        (F.col("price") / F.nullif("category_min_stock_qty_rounded", F.lit(0))).alias("price_per_category_min_stock_qty_rounded"),
    )
)

result = (
    base_products
    .select("brand", "cost", "brand_lower", "price_per_category_min_stock_qty_rounded")
)

from pyspark.sql import functions as F

stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("stock_qty").alias("min_product_stock_qty"))

matched_keys = (
    df_products
    .select("category")
    .intersect(
        df_products
        .filter(F.col("price") >= 690)
        .select("category")
    )
)

verified = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("launch_date") >= F.lit("2023-04-19").cast("date"))
    .select(
        "*",
        F.coalesce("price", F.lit(0)).alias("price_filled"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled"),
    )
)

joined = (
    cleaned
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .filter(F.col("p.rating") >= 3.9)
    .select("a.*", F.col("p.rating"))
)

not_flagged = (
    joined
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock") < F.col("s.launch_date")), "left_semi")
)

joined_products = (
    not_flagged
    .alias("src")
    .join(df_suppliers.alias("d"), F.col("src.supplier_id") == F.col("d.id"), "left")
    .select(
        "src.category",
        "src.brand",
        "src.price",
        "src.cost",
        "src.stock_qty",
        "src.tags",
        "src.supplier_id",
        "src.brand_filled",
        "src.rating",
        F.col("d.country"),
    )
)

joined_products_2 = (
    joined_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id").eqNullSafe(F.col("s.id")), "left")
    .select(
        "a.category",
        "a.price",
        "a.stock_qty",
        "a.supplier_id",
        "a.rating",
        F.col("s.supplier_name"),
        F.col("s.country").alias("supplier_country"),
    )
)

base = (
    joined_products_2
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "category",
        "price",
        "stock_qty",
        "supplier_id",
        "supplier_name",
        "supplier_country",
        F.concat("category", F.lit("/"), "supplier_name").alias("category_supplier_name"),
    )
)

wide = (
    base
    .groupBy("supplier_id", "category")
    .pivot("supplier_country", ["UK", "SG", "US"])
    .agg(F.avg("price"))
)

result = (
    wide
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select("supplier_id", "category", "UK", "US", F.col("min_product_stock_qty"))
)

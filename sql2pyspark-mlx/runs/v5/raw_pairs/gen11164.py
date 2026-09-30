from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-08-08")))
    .select(
        "*",
        F.upper("category").alias("category_upper"),
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity") != 5), "left_anti")
)

matched_keys = (
    not_flagged
    .select("brand")
    .exceptAll(
        df_products
        .filter(F.year("launch_date") >= 2024)
        .select("brand")
    )
)

reconciled = (
    not_flagged
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "inner")
    .filter(F.col("b.rating").isNull())
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.price",
        "s.cost",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        "s.stock_qty_filled",
        "s.resolved_brand",
        F.col("b.country"),
        F.col("b.city"),
        F.col("b.supplier_name"),
    )
)

with_dims_products = (
    with_dims
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "left")
    .select(
        "src.category",
        "src.price",
        "src.cost",
        "src.supplier_id",
        "src.stock_qty_filled",
        "src.country",
        "src.city",
        F.col("p.rating"),
        F.col("p.city").alias("supplier_city"),
        F.col("p.supplier_name").alias("supplier_supplier_name"),
    )
)

result = (
    with_dims_products
    .groupBy("city")
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.avg("stock_qty_filled").alias("avg_stock_qty_filled"),
    )
    .orderBy("avg_stock_qty_filled")
)

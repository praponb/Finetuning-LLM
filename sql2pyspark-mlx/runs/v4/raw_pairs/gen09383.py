from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-06-07").cast("date"))
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
        F.length("product_name").alias("product_name_length"),
    )
)

remaining_keys = (
    cleaned
    .select("category")
    .subtract(
        df_products
        .filter(F.col("supplier_id").isNotNull())
        .select("category")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.rating"), F.col("d.country"), F.col("d.supplier_name"))
)

filtered = (
    enriched
    .filter((F.col("launch_date") > F.lit("2023-05-07").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "stock_qty",
        "launch_date",
        "tags",
        "country",
        "supplier_name",
        F.round((F.col("price") * 0.85), 2).alias("adjusted_price"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("stock_qty") >= 420, "HIGH").otherwise("LOW").alias("stock_qty_tier"),
    )
    .agg(
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy(F.col("stock_qty_bit_and").desc_nulls_last())
)

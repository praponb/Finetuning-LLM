from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("id")
    .intersectAll(
        df_inventory
        .filter(F.col("product_id").isin([1, 5, 6, 8]))
        .select("id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter((F.col("last_restock") > F.lit("2023-06-20").cast("date")) & (F.col("quantity") > 101))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 1))
)

kept_keys_2 = (
    cleaned
    .select("store_id")
    .subtract(
        df_inventory
        .filter(F.year("last_restock") == 2025)
        .select("store_id")
    )
)

kept_rows_inventory = (
    cleaned
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.quantity",
        "src.last_restock_plus_months",
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("p.launch_date"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(~F.col("p.product_name").like("Y%"))
    .select(
        "a.product_id",
        "a.quantity",
        "a.last_restock_plus_months",
        F.col("p.cost"),
        F.col("p.launch_date").alias("product_launch_date"),
        F.col("p.brand").alias("product_brand"),
    )
)

joined_inventory = (
    joined
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select(
        "src.product_id",
        "src.quantity",
        "src.last_restock_plus_months",
        "src.cost",
        "src.product_brand",
        F.col("p.cost").alias("product_cost"),
        F.col("p.stock_qty"),
        F.col("p.price"),
    )
)

enriched_inventory = (
    joined_inventory
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.brand"))
)

result = (
    enriched_inventory
    .select(
        "product_id",
        "last_restock_plus_months",
        "product_brand",
        "product_cost",
        "price",
        "brand",
        F.when(F.col("product_brand").rlike("^(Acme|Initech)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("product_brand_zone"),
    )
    .orderBy(F.col("product_cost").desc(), F.col("product_brand").desc())
)

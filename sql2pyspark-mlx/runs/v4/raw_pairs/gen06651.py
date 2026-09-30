from pyspark.sql import functions as F

quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-08-16").cast("date")) & F.col("product_id").isin([4, 8]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.when(F.col("quantity") >= 267, "High").when(F.col("quantity") >= 54, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

remaining_keys = (
    valid
    .select("product_id", "id")
    .exceptAll(
        df_inventory
        .filter(F.col("last_restock").between("2023-12-01", "2024-10-01"))
        .select("product_id", "id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("quantity") >= 116)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_adjusted"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.product_name"))
)

filtered = (
    enriched
    .filter(F.col("product_id").isin([4, 5, 7, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_adjusted",
        F.initcap(F.trim("product_name")).alias("product_name_title"),
        (F.col("quantity") + F.col("quantity_adjusted")).alias("quantity_plus_quantity_adjusted"),
    )
)

base_inventory = (
    filtered
    .filter(F.col("quantity_adjusted") >= 73)
    .withColumn("quantity_per_quantity_adjusted", (F.col("quantity") / F.nullif("quantity_adjusted", F.lit(0))))
)

result = (
    base_inventory
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "product_name_title",
        F.col("avg_inventory_quantity"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)

from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-03-02"))) & (F.col("last_restock") < "2023-09-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.when(F.col("quantity") >= 233, "High").when(F.col("quantity") >= 49, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

valid = (
    filtered
    .filter(F.col("quantity").between(119, 172))
    .select(
        "*",
        F.floor("quantity").alias("quantity_whole"),
        F.ceil("quantity").alias("quantity_whole_2"),
    )
)

remaining_keys = (
    valid
    .select("store_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("product_id") == 9)
        .select("store_id", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys = (
    verified
    .select("product_id", "store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("product_id").isin([1, 5]))
        .select("product_id", "store_id")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("quantity") >= 206, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.percentile_approx("quantity_whole_2", [0.25, 0.5, 0.75]).alias("quantity_whole_2_quartiles"),
        F.max("quantity").alias("max_quantity"),
    )
    .orderBy(F.col("quantity_tier").desc())
)

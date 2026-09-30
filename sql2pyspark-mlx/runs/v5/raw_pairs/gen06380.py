from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.upper("product_name").like("%FFI%"))
    .withColumn("clean_category", F.trim("category"))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("UNKNOWN")))
)

cleaned_products = (
    cleaned
    .filter(F.col("brand") == "Umbrella")
    .select(
        "*",
        F.regexp_replace("product_name", " ", "_").alias("product_name_slug"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

remaining_keys = (
    cleaned_products
    .select("category", "id")
    .subtract(
        df_products
        .filter(F.array_contains("tags", "premium"))
        .select("category", "id")
    )
)

verified = (
    cleaned_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.city"))
)

result = (
    enriched
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "clean_category",
        "brand_clean",
        F.when(F.col("category").rlike("^(Sports|Books)$"), "OTHER_ZONE").when(F.col("category").rlike("^(Electronics|Home)$"), "PRIORITY").otherwise("CORE").alias("category_zone"),
    )
)

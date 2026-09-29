from pyspark.sql import functions as F

remaining_keys = (
    df_products
    .select("id", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("launch_date") < "2023-08-01")
        .select("id", "supplier_id")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") <= 186), "left_anti")
)

remaining_keys_2 = (
    eligible
    .select("id")
    .intersectAll(
        df_products
        .filter(F.col("cost").between(650, 670))
        .select("id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.array_contains("tags", "premium"))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        F.col("price").cast("int").alias("price_int"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

base = (
    filtered
    .filter((F.col("launch_date") >= F.lit("2023-06-06").cast("date")) & F.col("price_int").isNotNull())
    .select(
        "id",
        "brand",
        "price",
        "launch_date",
        "price_int",
        "brand_clean",
        F.round((F.col("cost") * 100), 2).alias("cost_adjusted"),
    )
)

base_products = (
    base
    .filter(F.col("launch_date") >= "2024-10-01")
    .select(
        "id",
        "brand",
        "launch_date",
        "price_int",
        F.round((F.col("price") * 1.1), 2).alias("adjusted_price"),
    )
)

totals = (
    base_products
    .groupBy("brand")
    .agg(
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_rows"),
        F.percentile_approx("adjusted_price", [0.25, 0.5, 0.75]).alias("adjusted_price_quartiles"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("globex_count") >= 1)
)

result = (
    totals
    .select(
        "brand",
        "globex_rows",
        "adjusted_price_quartiles",
        "distinct_ids",
        F.when(F.col("brand").rlike("^(Initech|Stark)$"), "GROUP_A").when(F.col("brand").rlike("^(Acme|Globex)$"), "GROUP_B").otherwise("CORE").alias("brand_zone"),
    )
)

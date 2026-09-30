from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-02-24").cast("date")) & (F.col("sale_date") < "2024-12-01"))
    .select(
        "*",
        F.date_sub("sale_date", 79).alias("sale_date_minus_79d"),
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.product_id",
        "src.sale_date",
        "src.units",
        "src.revenue",
        "src.sale_date_minus_79d",
        F.col("p.launch_date"),
        F.col("p.brand"),
    )
)

remaining_keys = (
    enriched
    .select("id", "product_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id").isin([1, 6, 7, 10]))
        .select("id", "product_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched_store_sales = (
    verified
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.category"))
)

result = (
    enriched_store_sales
    .select(
        "id",
        "units",
        "revenue",
        "sale_date_minus_79d",
        "launch_date",
        "brand",
        "category",
    )
    .orderBy(F.col("launch_date").desc())
)

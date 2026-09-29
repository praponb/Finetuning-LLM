from pyspark.sql import functions as F

joined = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "sale"))
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.sale_date",
        "src.revenue",
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("p.brand"),
    )
)

cleaned = (
    joined
    .filter((F.col("sale_date") >= F.lit("2023-05-17").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "cost",
        F.last_day("sale_date").alias("sale_date_month_end"),
        F.concat("product_name", F.lit(" - "), "brand").alias("product_name_brand"),
    )
)

valid = (
    cleaned
    .filter(F.month("sale_date_month_end").isin([2, 3, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "cost",
        "sale_date_month_end",
        "product_name_brand",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

enriched = (
    valid
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.id", "e.product_id", "e.revenue", F.col("p.brand"), F.col("p.category"))
)

joined_store_sales = (
    enriched
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.price"), F.col("b.stock_qty"))
)

matched_keys = (
    joined_store_sales
    .select("id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("revenue") >= 3200)
        .select("id", "product_id")
    )
)

verified = (
    joined_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("category").eqNullSafe(F.lit("Home")))
    .select(
        "product_id",
        "revenue",
        "price",
        "stock_qty",
        F.when(F.col("brand") == "Globex", 1).otherwise(0).alias("is_globex"),
        F.coalesce("brand", F.lit("N/A")).alias("resolved_brand"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("is_globex") >= 84, "PREMIUM").otherwise("STANDARD").alias("is_globex_tier"),
        F.when(F.col("stock_qty") >= 449, "LARGE").otherwise("SMALL").alias("stock_qty_tier"),
    )
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.percentile_approx("stock_qty", 0.5).alias("stock_qty_p50"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
)

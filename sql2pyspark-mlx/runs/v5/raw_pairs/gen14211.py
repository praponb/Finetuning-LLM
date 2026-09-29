from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-04-18").cast("date"))
    .withColumn("revenue_rounded", F.round("revenue", 2))
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
)

remaining_keys = (
    cleaned
    .select("product_id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("store_id").isin([3, 4, 6]))
        .select("product_id", "store_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select(
        "a.store_id",
        "a.product_id",
        "a.sale_date",
        "a.revenue_rounded",
        "a.sale_date_month_end",
        F.col("s.city"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.sqft"),
        F.col("b.city").alias("store_city"),
        F.col("b.store_name"),
    )
)

result = (
    with_dims
    .alias("s")
    .join(df_products.alias("dim"), F.col("s.product_id") == F.col("dim.id"), "left")
    .rollup(
        F.col("dim.category").alias("category"),
        F.col("s.store_city").alias("store_city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.revenue_rounded")).alias("min_revenue_rounded"),
        F.max(F.col("s.sqft")).alias("max_sqft"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("store_city").desc())
)

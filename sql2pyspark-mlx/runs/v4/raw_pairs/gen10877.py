from pyspark.sql import functions as F

with_dims = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.region"), F.col("p.opened_date"), F.col("p.sqft"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select(
        "s.*",
        F.col("p.opened_date").alias("store_opened_date"),
        F.col("p.region").alias("store_region"),
        F.col("p.store_name"),
    )
)

with_dims_store_sales = (
    enriched
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.launch_date"), F.col("p.cost"), F.col("p.brand"))
)

stats = (
    with_dims_store_sales
    .groupBy(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.round(F.sum("units") / F.nullif(F.count(F.when(F.col("brand") == "Initech", 1)), F.lit(0)), 2).alias("units_per_initech"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_rows"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.size(F.collect_set("brand")).alias("num_brands"),
    )
)

result = (
    stats
    .select(
        "quarter_start",
        "total_revenue",
        "units_per_initech",
        "umbrella_rows",
        "revenue_quartiles",
        "num_brands",
    )
)

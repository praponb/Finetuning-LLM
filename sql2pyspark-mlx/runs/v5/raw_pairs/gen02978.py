from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("avg_revenue").desc())

valid = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-05-20").cast("date")) & F.col("sale_date").between("2023-05-01", "2024-01-09"))
    .withColumn("revenue_per_unit", (F.col("revenue") * F.col("units")))
)

remaining_keys = (
    valid
    .select("store_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("store_id").isin([1, 3, 6]))
        .select("store_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select(
        "a.product_id",
        "a.sale_date",
        "a.units",
        "a.revenue",
        "a.revenue_per_unit",
        F.col("p.category"),
        F.col("p.brand"),
        F.col("p.product_name"),
    )
)

matched_keys = (
    with_dims
    .select("product_id")
    .intersect(
        df_store_sales
        .filter(F.col("store_id").isin([1, 3, 4, 5]))
        .select("product_id")
    )
)

kept_rows_store_sales = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_dims_store_sales = (
    kept_rows_store_sales
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.price") <= 530)
    .select(
        "e.product_id",
        "e.revenue",
        "e.revenue_per_unit",
        "e.brand",
        "e.product_name",
        F.col("p.product_name").alias("product_product_name"),
        F.col("p.cost"),
    )
)

daily = (
    with_dims_store_sales
    .groupBy("product_id", "brand")
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("revenue_per_unit", 0.9).alias("revenue_per_unit_p90"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .withColumn("avg_revenue_rank", F.rank().over(w))
)

result = (
    daily
    .select("product_id", "brand", "event_count", "avg_revenue", "avg_revenue_rank")
)

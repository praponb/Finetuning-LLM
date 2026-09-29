from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy(F.col("cost").desc_nulls_last(), "id")
w2 = Window.partitionBy("supplier_id", "category").orderBy(F.col("launch_date").desc(), "id")

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-05-24").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.last_day("launch_date").alias("launch_date_month_end"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id") == 1), "left_anti")
)

latest = (
    eligible
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

kept = (
    latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.launch_date")), "left_semi")
)

deduped = (
    kept
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "category",
        "stock_qty",
        "supplier_id",
        "launch_date_month_end",
        "brand_filled",
        "latest_rank",
    )
)

stats = (
    deduped
    .rollup(
        F.when(F.col("stock_qty") >= 320, "PREMIUM").otherwise("STANDARD").alias("stock_qty_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
        F.countDistinct("category").alias("distinct_categories"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_rows"),
        F.percentile_approx("stock_qty", 0.5).alias("stock_qty_p50"),
    )
    .filter(F.col("row_count") > 1)
)

result = (
    stats
    .select(
        "row_count",
        "home_count",
        "distinct_categories",
        "home_rows",
        "stock_qty_p50",
    )
    .orderBy(F.col("distinct_categories").desc_nulls_last(), "row_count")
)

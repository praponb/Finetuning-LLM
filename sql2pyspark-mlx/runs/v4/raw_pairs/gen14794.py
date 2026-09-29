from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("category", "supplier_id").orderBy(F.col("stock_qty").desc())
w2 = Window.partitionBy("supplier_id", "brand")

remaining_keys = (
    df_products
    .select("supplier_id")
    .intersect(
        df_products
        .filter(F.col("launch_date") <= "2025-10-01")
        .select("supplier_id")
    )
)

verified = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.percent_rank().over(w_ordered).alias("stock_qty_percent_rank"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
    )
)

with_dims = (
    with_windows
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.rating"))
)

cleaned = (
    with_dims
    .filter((F.col("launch_date") > F.lit("2023-12-10").cast("date")) & F.col("supplier_id").isNotNull())
    .withColumn("brand_category_key", F.concat_ws("/", "brand", "category"))
    .withColumn("price_plus_price_share", (F.col("price") + F.col("price_share")))
)

daily = (
    cleaned
    .withColumn("week_start", F.date_trunc("WEEK", "launch_date"))
    .withColumn("stock_qty_tier", F.when(F.col("stock_qty") >= 420, "HIGH").otherwise("LOW"))
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.6, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["week_start", "stock_qty_tier", "rating_tier"], ["week_start", "stock_qty_tier"]],
        "week_start",
        "stock_qty_tier",
        "rating_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.grouping("week_start").alias("grouping_id"),
    )
)

result = (
    daily
    .select(
        "stock_qty_tier",
        "rating_tier",
        "distinct_ids",
        "stock_qty_quartiles",
        "rating_quartiles",
        "grouping_id",
    )
)

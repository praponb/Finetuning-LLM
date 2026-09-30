from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("margin").desc(), "id")
w2 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand")
w4 = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")

filtered = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-07-27").cast("date")) & F.col("supplier_id").isNotNull())
    .withColumn("margin", F.round((F.col("price") / F.col("cost")), 2))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("OTHER")))
)

metrics = (
    filtered
    .filter(F.col("stock_qty") < 154)
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        "margin",
        F.ntile(10).over(w).alias("margin_bucket"),
        F.sum("margin").over(w2).alias("running_margin"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id") == 5), "left_semi")
)

scored = (
    not_flagged
    .select(
        "id",
        "cost",
        "running_margin",
        F.count("running_margin").over(w3).alias("brand_count_running_margin"),
        F.ntile(5).over(w4).alias("stock_qty_bucket"),
    )
)

not_flagged_products = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 3, 5, 6])), "left_semi")
)

kept = (
    not_flagged_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([5, 8, 12, 13])), "left_semi")
)

remaining_keys = (
    kept
    .select("id")
    .subtract(
        df_products
        .filter(F.col("cost") > 580)
        .select("id")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

summary = (
    reconciled
    .withColumn("brand_count_running_margin_tier", F.when(F.col("brand_count_running_margin") >= 13, "LARGE").otherwise("SMALL"))
    .withColumn("cost_tier", F.when(F.col("cost") >= 620, "PREMIUM").otherwise("STANDARD"))
    .cube("brand_count_running_margin_tier", "cost_tier")
    .agg(
        F.min("running_margin").alias("min_running_margin"),
        F.percentile_approx("stock_qty_bucket", 0.95).alias("stock_qty_bucket_p95"),
        F.grouping("brand_count_running_margin_tier").alias("grouping_level"),
    )
)

result = summary.select("brand_count_running_margin_tier", "cost_tier", "min_running_margin")

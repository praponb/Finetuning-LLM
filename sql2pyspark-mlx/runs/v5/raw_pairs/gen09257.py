from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("country").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

eligible = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price").isNotNull()), "left_semi")
)

with_dims = (
    eligible
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "inner")
    .filter(F.col("b.city") != "Tokyo")
    .select(
        "e.id",
        "e.product_name",
        "e.brand",
        "e.stock_qty",
        "e.tags",
        "e.supplier_id",
        F.col("b.supplier_name"),
        F.col("b.city"),
        F.col("b.country"),
    )
)

with_windows = (
    with_dims
    .withColumn("running_count_stock_qty", F.count("stock_qty").over(w_ordered))
    .withColumn("running_min_stock_qty", F.min("stock_qty").over(w_ordered))
    .withColumn("running_avg_stock_qty", F.avg("stock_qty").over(w_ordered))
    .withColumn("cumulative_stock_qty", F.sum("stock_qty").over(w_hist))
)

eligible_products = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.order_id") == 14), "left_anti")
)

enriched = (
    eligible_products
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "inner")
    .select(
        "e.id",
        "e.stock_qty",
        "e.tags",
        "e.supplier_id",
        "e.supplier_name",
        "e.running_min_stock_qty",
        "e.running_avg_stock_qty",
        F.col("s.city").alias("supplier_city"),
        F.col("s.country").alias("supplier_country"),
    )
)

result = (
    enriched
    .withColumn("running_avg_stock_qty_tier", F.when(F.col("running_avg_stock_qty") >= 390, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["running_avg_stock_qty_tier", "supplier_id"], ["running_avg_stock_qty_tier"]],
        "running_avg_stock_qty_tier",
        "supplier_id",
    )
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.sum(F.when(F.col("supplier_country") == "UK", F.col("stock_qty"))).alias("uk_total_stock_qty"),
        F.count("*").alias("cnt"),
        F.percentile_approx("running_avg_stock_qty", [0.25, 0.5, 0.75]).alias("running_avg_stock_qty_quartiles"),
        F.round(F.avg("running_avg_stock_qty"), 2).alias("avg_running_avg_stock_qty"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("supplier_id").asc_nulls_last())
)

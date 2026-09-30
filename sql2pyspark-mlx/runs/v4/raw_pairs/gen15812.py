from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand")
w3 = Window.partitionBy("supplier_id", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

eligible = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price") > 170), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.launch_date")), "left_anti")
)

with_windows = (
    kept
    .withColumn("cumulative_price", F.sum("price").over(w_running))
    .withColumn("brand_sum_stock_qty", F.sum("stock_qty").over(w2))
    .withColumn("running_stock_qty", F.sum("stock_qty").over(w3))
)

stats = (
    with_windows
    .withColumn("activity_month", F.date_trunc("MONTH", "launch_date"))
    .rollup("activity_month", "category")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", F.col("cumulative_price"))).alias("globex_total_cumulative_price"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.percentile_approx("stock_qty", 0.95).alias("stock_qty_p95"),
        F.count("*").alias("row_count"),
        F.grouping("activity_month").alias("grouping_level"),
    )
)

result = (
    stats
    .select(
        "activity_month",
        "category",
        "globex_total_cumulative_price",
        "category_list",
        "stock_qty_bit_and",
        "stock_qty_p95",
        "row_count",
        "grouping_level",
    )
)

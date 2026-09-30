from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("product_stock_qty", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("cost").desc())
w3 = Window.partitionBy("category").orderBy("last3_count_quantity", "id")
w4 = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("first_known_last3_sum_quantity_per_stock_qty", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("category")

with_dims = (
    df_order_items
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.category") != "Grocery")
    .select("e.*", F.col("b.cost"), F.col("b.price"), F.col("b.stock_qty"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.product_id",
        "s.quantity",
        "s.cost",
        "s.stock_qty",
        F.col("p.stock_qty").alias("product_stock_qty"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "product_id",
        "stock_qty",
        F.count("quantity").over(w).alias("last3_count_quantity"),
        F.min("quantity").over(w).alias("last3_min_quantity"),
        F.sum("quantity").over(w).alias("last3_sum_quantity"),
        F.rank().over(w2).alias("cost_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("cost_rank").between(6, 8))
    .withColumn("last3_sum_quantity_per_stock_qty", (F.col("last3_sum_quantity") / F.nullif("stock_qty", F.lit(0))))
    .withColumn("stock_qty_level", F.when(F.col("stock_qty") >= 441, "High").when(F.col("stock_qty") >= 137, "Medium").otherwise("Low"))
)

enriched_order_items = (
    cleaned
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.category"))
)

metrics_order_items = (
    enriched_order_items
    .withColumn("prev_stock_qty", F.lag("stock_qty").over(w3))
    .withColumn("next_stock_qty", F.lead("stock_qty").over(w3))
    .withColumn("next_last3_count_quantity", F.lead("last3_count_quantity").over(w3))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("first_known_last3_sum_quantity_per_stock_qty", F.first("last3_sum_quantity_per_stock_qty", ignorenulls=True).over(w4))
)

with_windows = (
    metrics_order_items
    .select(
        "id",
        "product_id",
        "stock_qty",
        "last3_min_quantity",
        "last3_sum_quantity_per_stock_qty",
        "stock_qty_level",
        "prev_stock_qty",
        F.avg("cost_rank").over(w5).alias("last7_avg_cost_rank"),
        F.min("cost_rank").over(w5).alias("last7_min_cost_rank"),
        F.max("cost_rank").over(w5).alias("last7_max_cost_rank"),
        F.count("cost_rank").over(w5).alias("last7_count_cost_rank"),
        F.stddev_samp("last3_sum_quantity_per_stock_qty").over(w6).alias("category_stddev_samp_last3_sum_quantity_per_stock_qty"),
    )
)

grouped = (
    with_windows
    .withColumn("category_stddev_samp_last3_sum_quantity_per_stock_qty_tier", F.when(F.col("category_stddev_samp_last3_sum_quantity_per_stock_qty") >= 67, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["product_id", "category_stddev_samp_last3_sum_quantity_per_stock_qty_tier"], ["product_id"], []],
        "product_id",
        "category_stddev_samp_last3_sum_quantity_per_stock_qty_tier",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("last7_max_cost_rank", [0.25, 0.5, 0.75]).alias("last7_max_cost_rank_quartiles"),
    )
)

result = (
    grouped
    .select("row_count")
    .orderBy(F.col("row_count").desc())
)

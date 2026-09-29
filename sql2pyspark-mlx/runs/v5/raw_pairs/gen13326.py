from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("category").orderBy("price", "id").rowsBetween(-5, -1)
w_hist = Window.partitionBy("supplier_id").orderBy("price", "id")
w3 = Window.partitionBy("category").orderBy("cost_filled")
w4 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("category").orderBy("stock_qty_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("category").orderBy("prev_cost_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-09-30").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "stock_qty",
        "tags",
        "supplier_id",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.concat("category", F.lit(", "), "brand").alias("category_brand"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "category",
        "tags",
        F.avg("stock_qty").over(w_running).alias("stock_qty_avg_5_prior"),
        F.lag("price", 2).over(w_hist).alias("prev_price"),
        F.cume_dist().over(w3).alias("cost_filled_cume_dist"),
        F.last("stock_qty", ignorenulls=True).over(w4).alias("last_known_stock_qty"),
        F.lag("cost_filled", 1).over(w_hist).alias("prev_cost_filled"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isNotNull()), "left_anti")
)

enriched = (
    eligible
    .select(
        "*",
        F.first("last_known_stock_qty", ignorenulls=True).over(w5).alias("first_known_last_known_stock_qty"),
        F.last("cost_filled_cume_dist", ignorenulls=True).over(w6).alias("last_known_cost_filled_cume_dist"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("stock_qty_avg_5_prior") >= 323, "HIGH").otherwise("LOW").alias("stock_qty_avg_5_prior_tier"),
    )
    .agg(
        F.count_if(F.col("category") == "Fashion").alias("fashion_events"),
        F.percentile_approx("prev_cost_filled", [0.25, 0.5, 0.75]).alias("prev_cost_filled_quartiles"),
        F.min("cost_filled_cume_dist").alias("min_cost_filled_cume_dist"),
    )
)

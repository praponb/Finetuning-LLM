from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("price", "id")
w2 = Window.partitionBy("brand", "supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("brand").orderBy("price", "id")
w5 = Window.orderBy("inventory_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_min_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("next_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("running_max_stock_qty").desc(), "id")

metrics = (
    df_products
    .filter(F.col("supplier_id") == 37)
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        F.lag("cost").over(w).alias("prev_cost"),
        F.lag("price").over(w).alias("prev_price"),
        F.row_number().over(w).alias("event_seq"),
        F.first("price", ignorenulls=True).over(w2).alias("first_known_price"),
    )
)

cleaned = (
    metrics
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-02-21"))) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "prev_price",
        "event_seq",
        F.when(F.col("first_known_price") >= 980, "High").when(F.col("first_known_price") >= 520, "Medium").otherwise("Low").alias("first_known_price_band"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

with_windows = (
    with_latest
    .select(
        "id",
        "price",
        "stock_qty",
        "event_seq",
        "first_known_price_band",
        "inventory_last_restock",
        F.lead("stock_qty").over(w_hist).alias("next_stock_qty"),
        F.lag("event_seq").over(w_hist).alias("prev_event_seq"),
        F.lead("event_seq").over(w_hist).alias("next_event_seq"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

metrics_products = (
    with_windows
    .select(
        "id",
        "stock_qty",
        "first_known_price_band",
        "inventory_last_restock",
        "next_event_seq",
        F.min("stock_qty").over(w5).alias("running_min_stock_qty"),
        F.avg("stock_qty").over(w5).alias("running_avg_stock_qty"),
        F.max("stock_qty").over(w5).alias("running_max_stock_qty"),
    )
)

scored = (
    metrics_products
    .select(
        "id",
        "inventory_last_restock",
        F.first("running_max_stock_qty", ignorenulls=True).over(w6).alias("first_known_running_max_stock_qty"),
        F.last("running_min_stock_qty", ignorenulls=True).over(w7).alias("last_known_running_min_stock_qty"),
        F.ntile(5).over(w8).alias("running_max_stock_qty_bucket"),
        F.last("next_event_seq", ignorenulls=True).over(w7).alias("last_known_next_event_seq"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.store_id").isin([2, 3, 6])), "left_semi")
)

result = (
    eligible
    .groupBy(F.date_trunc("WEEK", "inventory_last_restock").alias("week_start"))
    .agg(
        F.percentile_approx("running_max_stock_qty_bucket", 0.5).alias("running_max_stock_qty_bucket_p50"),
        F.round(F.avg("last_known_next_event_seq"), 2).alias("avg_last_known_next_event_seq"),
        F.percentile_approx("last_known_next_event_seq", 0.9).alias("last_known_next_event_seq_p90"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

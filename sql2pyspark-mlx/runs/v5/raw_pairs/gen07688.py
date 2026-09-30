from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy(F.col("unit_price").desc(), "id")
w3 = Window.partitionBy("supplier_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category").orderBy("stock_qty_whole", "id")
w5 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc_nulls_last())
w6 = Window.partitionBy("category").orderBy(F.col("unit_price").desc_nulls_last())
w7 = Window.orderBy("event_seq", "id")
w8 = Window.partitionBy("supplier_id").orderBy("prev_stock_qty", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("supplier_id").orderBy(F.col("unit_price_cume_dist").desc(), "id")
w10 = Window.partitionBy("supplier_id")

filtered = (
    df_products
    .filter(F.col("supplier_id").isin([16, 37, 40]))
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "tags",
        "supplier_id",
        F.col("price").cast("int").alias("price_int"),
    )
)

cleaned = (
    filtered
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "tags",
        "supplier_id",
        "price_int",
        F.floor("stock_qty").alias("stock_qty_whole"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

scored = (
    with_latest
    .select(
        "id",
        "product_name",
        "supplier_id",
        "price_int",
        "stock_qty_whole",
        "unit_price",
        F.row_number().over(w2).alias("event_seq"),
        F.sum("stock_qty_whole").over(w3).alias("cumulative_stock_qty_whole"),
        F.lag("stock_qty", 2).over(w4).alias("prev_stock_qty"),
        F.rank().over(w5).alias("stock_qty_rank"),
        F.cume_dist().over(w6).alias("unit_price_cume_dist"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.first("cumulative_stock_qty_whole", ignorenulls=True).over(w3).alias("first_known_cumulative_stock_qty_whole"),
        F.row_number().over(w7).alias("seq_no"),
        F.avg("event_seq").over(w8).alias("event_seq_avg_3"),
        F.ntile(5).over(w9).alias("unit_price_cume_dist_bucket"),
        F.round((F.col("stock_qty_rank") / F.sum("stock_qty_rank").over(w10)), 4).alias("stock_qty_rank_share"),
    )
)

result = (
    metrics
    .cube("supplier_id", "seq_no")
    .agg(
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
        F.round(F.avg("stock_qty_whole"), 2).alias("avg_stock_qty_whole"),
        F.grouping("supplier_id").alias("grouping_id"),
    )
)

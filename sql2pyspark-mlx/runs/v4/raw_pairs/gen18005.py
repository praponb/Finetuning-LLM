from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("price", "id")
w2 = Window.partitionBy("supplier_id")
w3 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc(), "id")
w_hist = Window.partitionBy("category").orderBy("launch_date", "id")
w5 = Window.partitionBy("category", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("brand").orderBy("launch_date", "id")
w7 = Window.orderBy("launch_date", "id")
w8 = Window.partitionBy("brand")

base = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "*",
        F.month("launch_date").alias("launch_date_month"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.ntile(5).over(w).alias("price_bucket"),
        F.min("stock_qty").over(w2).alias("supplier_id_min_stock_qty"),
        F.ntile(5).over(w3).alias("stock_qty_bucket"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

enriched = (
    with_windows
    .withColumn("first_known_stock_qty", F.first("stock_qty", ignorenulls=True).over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
)

filtered = (
    enriched
    .filter(F.col("stock_qty_bucket").between(3, 6))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "price_bucket",
        "supplier_id_min_stock_qty",
        "stock_qty_bucket",
        F.floor("stock_qty").alias("stock_qty_whole"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.ntile(10).over(w3).alias("stock_qty_bucket_2"),
        F.row_number().over(w7).alias("event_seq"),
        F.max("stock_qty_bucket").over(w8).alias("brand_max_stock_qty_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "price",
        "launch_date",
        "supplier_id_min_stock_qty",
        "stock_qty_bucket",
        "stock_qty_whole",
        "stock_qty_bucket_2",
        "event_seq",
        "brand_max_stock_qty_bucket",
    )
)

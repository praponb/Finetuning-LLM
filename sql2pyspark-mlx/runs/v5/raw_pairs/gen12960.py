from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id").orderBy("quantity_filled", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("product_id").orderBy(F.col("last7_min_unit_price").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w4 = Window.orderBy("last7_max_unit_price", "id")
w_recent = Window.partitionBy("product_id").orderBy("last7_avg_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id")

valid = (
    df_order_items
    .filter(F.col("quantity").between(5, 7))
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.greatest("unit_price", "quantity").alias("greatest_unit_price_quantity"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

with_windows = (
    valid
    .filter(F.col("greatest_unit_price_quantity") >= 78)
    .select(
        "*",
        F.max("unit_price").over(w_hist).alias("last7_max_unit_price"),
        F.min("unit_price").over(w_hist).alias("last7_min_unit_price"),
        F.avg("unit_price").over(w_hist).alias("last7_avg_unit_price"),
    )
)

with_windows_order_items = (
    with_windows
    .select(
        "id",
        "product_id",
        "quantity",
        "greatest_unit_price_quantity",
        F.ntile(10).over(w_running).alias("last7_min_unit_price_bucket"),
        F.row_number().over(w3).alias("row_seq"),
        F.lag("unit_price", 2).over(w4).alias("prev_unit_price"),
        F.last("greatest_unit_price_quantity", ignorenulls=True).over(w_recent).alias("last_known_greatest_unit_price_quantity"),
        F.avg("quantity_filled").over(w6).alias("product_id_avg_quantity_filled"),
    )
)

with_dims = (
    with_windows_order_items
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.product_id",
        "a.greatest_unit_price_quantity",
        "a.row_seq",
        "a.prev_unit_price",
        "a.product_id_avg_quantity_filled",
        F.col("b.launch_date"),
        F.col("b.stock_qty"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "greatest_unit_price_quantity",
        "row_seq",
        "product_id_avg_quantity_filled",
        "launch_date",
        "stock_qty",
    )
    .orderBy(F.col("row_seq").desc(), F.col("stock_qty").desc())
)

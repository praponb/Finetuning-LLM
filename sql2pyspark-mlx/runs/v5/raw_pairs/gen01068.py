from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w_ordered = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("last_known_unit_price").desc(), "id")
w5 = Window.orderBy(F.col("seq_no").desc(), "id")
w6 = Window.orderBy("quantity_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("last_known_unit_price_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("running_avg_last_known_unit_price", "id")
w9 = Window.orderBy(F.col("running_avg_last_known_unit_price").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "*",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
        (F.col("quantity") + F.col("unit_price")).alias("line_total_2"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "quantity",
        "line_total",
        "line_total_2",
        F.avg("quantity").over(w_part).alias("quantity_avg_6"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.last("unit_price", ignorenulls=True).over(w_ordered).alias("last_known_unit_price"),
    )
)

enriched = (
    with_windows
    .withColumn("last_known_unit_price_bucket", F.ntile(5).over(w4))
    .withColumn("seq_no_2", F.row_number().over(w5))
    .withColumn("first_known_quantity_avg_6", F.first("quantity_avg_6", ignorenulls=True).over(w6))
    .withColumn("seq_no_bucket", F.ntile(4).over(w5))
)

scored = (
    enriched
    .select(
        "id",
        "quantity",
        "line_total",
        "seq_no",
        "last_known_unit_price_bucket",
        "seq_no_bucket",
        F.avg("last_known_unit_price").over(w7).alias("running_avg_last_known_unit_price"),
        F.min("last_known_unit_price").over(w7).alias("running_min_last_known_unit_price"),
        F.count("last_known_unit_price").over(w7).alias("running_count_last_known_unit_price"),
    )
)

base = (
    scored
    .filter(F.col("seq_no") != 8)
    .select(
        "id",
        "line_total",
        "seq_no",
        "seq_no_bucket",
        "running_avg_last_known_unit_price",
        "running_count_last_known_unit_price",
        F.coalesce("seq_no_bucket", F.lit(0)).alias("seq_no_bucket_filled"),
    )
)

with_windows_order_items = (
    base
    .select(
        "*",
        F.lag("running_avg_last_known_unit_price").over(w8).alias("prev_running_avg_last_known_unit_price"),
        F.lag("seq_no_bucket_filled").over(w8).alias("prev_seq_no_bucket_filled"),
        F.lead("seq_no_bucket_filled").over(w8).alias("next_seq_no_bucket_filled"),
        F.row_number().over(w8).alias("seq_no_2"),
        F.row_number().over(w9).alias("row_seq"),
    )
)

cleaned = (
    with_windows_order_items
    .filter(F.col("next_seq_no_bucket_filled") > 66)
    .select(
        "*",
        (F.col("prev_running_avg_last_known_unit_price") / F.nullif("line_total", F.lit(0))).alias("prev_running_avg_last_known_unit_price_per_line_total"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "line_total",
        "seq_no",
        "running_avg_last_known_unit_price",
        "running_count_last_known_unit_price",
        "seq_no_bucket_filled",
        "prev_seq_no_bucket_filled",
        "seq_no_2",
        "prev_running_avg_last_known_unit_price_per_line_total",
    )
    .orderBy("running_count_last_known_unit_price")
)

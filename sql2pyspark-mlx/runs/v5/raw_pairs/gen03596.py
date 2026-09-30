from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("unit_price_adjusted", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("unit_price_rounded", "id").rowsBetween(-3, -1)
w3 = Window.orderBy("last4_count_quantity", "id")
w4 = Window.orderBy("adjusted_unit_price_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("order_id") == 28)
    .select(
        "id",
        "quantity",
        "unit_price",
        F.round("unit_price", 0).alias("unit_price_rounded"),
    )
)

base_order_items = (
    base
    .filter(F.col("unit_price") <= 110)
    .select(
        "*",
        F.round((F.col("unit_price") * 1.07), 2).alias("unit_price_adjusted"),
        F.round((F.col("unit_price_rounded") * 0.85), 2).alias("adjusted_unit_price_rounded"),
    )
)

with_windows = (
    base_order_items
    .select(
        "*",
        F.count("quantity").over(w_running).alias("last4_count_quantity"),
        F.min("quantity").over(w_running).alias("last4_min_quantity"),
        F.avg("quantity").over(w_running).alias("last4_avg_quantity"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.avg("adjusted_unit_price_rounded").over(w2).alias("adjusted_unit_price_rounded_avg_3_prior"),
        F.ntile(3).over(w3).alias("last4_count_quantity_bucket"),
        F.last("last4_avg_quantity", ignorenulls=True).over(w4).alias("last_known_last4_avg_quantity"),
    )
)

valid = (
    metrics
    .filter(F.col("adjusted_unit_price_rounded") <= 44)
    .select(
        "id",
        "unit_price",
        "unit_price_rounded",
        "unit_price_adjusted",
        "adjusted_unit_price_rounded",
        "last4_count_quantity",
        "last4_min_quantity",
        "last4_avg_quantity",
        "adjusted_unit_price_rounded_avg_3_prior",
        (F.col("last4_count_quantity") * F.col("quantity")).alias("last4_count_quantity_times_quantity"),
    )
)

result = (
    valid
    .select(
        "id",
        "unit_price_rounded",
        "adjusted_unit_price_rounded",
        "last4_count_quantity",
        "last4_avg_quantity",
        "adjusted_unit_price_rounded_avg_3_prior",
        "last4_count_quantity_times_quantity",
    )
)

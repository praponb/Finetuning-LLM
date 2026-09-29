from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id")

scored = (
    df_order_items
    .select(
        "*",
        F.avg("quantity").over(w_part).alias("running_avg_quantity"),
        F.min("quantity").over(w_part).alias("running_min_quantity"),
        F.sum("quantity").over(w_part).alias("running_sum_quantity"),
        F.max("quantity").over(w_part).alias("running_max_quantity"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

remaining_keys = (
    scored
    .select("order_id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("unit_price").between(150, 240))
        .select("order_id", "product_id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("quantity") == 8)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "running_avg_quantity",
        "running_min_quantity",
        "running_sum_quantity",
        "event_seq",
        F.when(F.col("running_max_quantity") >= 8.6, "High").when(F.col("running_max_quantity") >= 2.1, "Medium").otherwise("Low").alias("running_max_quantity_band"),
    )
)

filtered = (
    valid
    .filter(F.col("event_seq") >= 5)
    .select(
        "id",
        "quantity",
        "unit_price",
        "running_min_quantity",
        F.round("running_avg_quantity", 2).alias("running_avg_quantity_rounded"),
    )
)

valid_order_items = (
    filtered
    .filter(F.col("unit_price").between(100, 330))
    .select(
        "*",
        (F.col("running_min_quantity") * F.col("running_avg_quantity_rounded")).alias("running_min_quantity_times_running_avg_quantity_rounded"),
    )
)

result = valid_order_items.select("quantity", "running_avg_quantity_rounded")

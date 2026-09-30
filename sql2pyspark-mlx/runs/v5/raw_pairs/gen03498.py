from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity_filled", "id")
w2 = Window.orderBy("last_restock_month_end", "id")
w3 = Window.partitionBy("city").orderBy("last_restock", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("last_restock_month_end").desc(), "id")
w5 = Window.partitionBy("city").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("city").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("next_quantity_filled").desc_nulls_first())
w8 = Window.orderBy("next_quantity_filled_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("running_count_running_quantity_filled", "id")
w_grp = Window.orderBy("prev_quantity_filled", "id")
w_running = Window.orderBy("running_min_prev_quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("running_count_running_quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy(F.col("running_min_prev_quantity_filled").desc(), "id")

cleaned = (
    df_inventory
    .filter(F.col("product_id") == 7)
    .select(
        "id",
        "store_id",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.store_id",
        "e.last_restock",
        "e.last_restock_month_end",
        "e.quantity_filled",
        F.col("b.city"),
    )
)

scored = (
    with_dims
    .select(
        "id",
        "last_restock_month_end",
        "quantity_filled",
        "city",
        F.ntile(4).over(w).alias("quantity_filled_bucket"),
        F.lead("quantity_filled", 2).over(w2).alias("next_quantity_filled"),
        F.lag("quantity_filled", 1).over(w3).alias("prev_quantity_filled"),
        F.row_number().over(w4).alias("seq_no"),
        F.sum("quantity_filled").over(w5).alias("running_quantity_filled"),
    )
)

scored_inventory = (
    scored
    .select(
        "id",
        "prev_quantity_filled",
        F.avg("running_quantity_filled").over(w_ordered).alias("running_avg_running_quantity_filled"),
        F.sum("running_quantity_filled").over(w_ordered).alias("running_sum_running_quantity_filled"),
        F.count("running_quantity_filled").over(w_ordered).alias("running_count_running_quantity_filled"),
        F.dense_rank().over(w7).alias("next_quantity_filled_dense_rank"),
    )
)

metrics = (
    scored_inventory
    .select(
        "*",
        F.max("prev_quantity_filled").over(w8).alias("running_max_prev_quantity_filled"),
        F.min("prev_quantity_filled").over(w8).alias("running_min_prev_quantity_filled"),
        F.count("prev_quantity_filled").over(w8).alias("running_count_prev_quantity_filled"),
    )
)

with_windows = (
    metrics
    .filter(F.col("running_max_prev_quantity_filled") < 48)
    .select(
        "*",
        F.lead("running_sum_running_quantity_filled", 1).over(w9).alias("next_running_sum_running_quantity_filled"),
        F.lag("running_max_prev_quantity_filled", 2).over(w_grp).alias("prev_running_max_prev_quantity_filled"),
        F.first("running_avg_running_quantity_filled", ignorenulls=True).over(w_running).alias("first_known_running_avg_running_quantity_filled"),
        F.sum("next_quantity_filled_dense_rank").over(w12).alias("cumulative_next_quantity_filled_dense_rank"),
        F.row_number().over(w13).alias("seq_no"),
    )
)

valid = (
    with_windows
    .filter(F.col("running_count_prev_quantity_filled") < 13)
    .select(
        "*",
        (F.col("running_min_prev_quantity_filled") / F.nullif("running_avg_running_quantity_filled", F.lit(0))).alias("running_min_prev_quantity_filled_per_running_avg_running_quantity_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "prev_quantity_filled",
        "running_avg_running_quantity_filled",
        "running_sum_running_quantity_filled",
        "running_count_running_quantity_filled",
        "running_max_prev_quantity_filled",
        "running_min_prev_quantity_filled",
        "running_count_prev_quantity_filled",
        "next_running_sum_running_quantity_filled",
    )
)

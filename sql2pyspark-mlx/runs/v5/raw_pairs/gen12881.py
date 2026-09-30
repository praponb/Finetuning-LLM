from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("cumulative_quantity", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("running_sum_unit_price").desc_nulls_last())

with_windows = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        F.min("unit_price").over(w_grp).alias("running_min_unit_price"),
        F.avg("unit_price").over(w_grp).alias("running_avg_unit_price"),
        F.sum("unit_price").over(w_grp).alias("running_sum_unit_price"),
        F.sum("quantity").over(w_grp).alias("cumulative_quantity"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.product_name").like("R%"))
    .select("s.*", F.col("p.category"))
)

with_windows_order_items = (
    with_dims
    .withColumn("next_running_min_unit_price", F.lead("running_min_unit_price").over(w2))
    .withColumn("prev_running_min_unit_price", F.lag("running_min_unit_price").over(w2))
    .withColumn("prev_running_sum_unit_price", F.lag("running_sum_unit_price").over(w2))
    .withColumn("next_running_sum_unit_price", F.lead("running_sum_unit_price").over(w2))
    .withColumn("event_seq", F.row_number().over(w2))
)

ranked = (
    with_windows_order_items
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "order_id",
        "product_id",
        "running_sum_unit_price",
        "cumulative_quantity",
        "next_running_min_unit_price",
        "prev_running_min_unit_price",
        "prev_running_sum_unit_price",
        "next_running_sum_unit_price",
    )
)

cleaned = (
    ranked
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.col("running_sum_unit_price").cast("int").alias("running_sum_unit_price_int"),
        F.round("cumulative_quantity", 2).alias("cumulative_quantity_rounded"),
    )
)

remaining_keys = (
    cleaned
    .select("product_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") >= 4)
        .select("product_id", "id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "running_sum_unit_price",
        "prev_running_min_unit_price",
        "next_running_sum_unit_price",
        "cumulative_quantity_rounded",
    )
    .orderBy(F.col("id").desc_nulls_last())
)

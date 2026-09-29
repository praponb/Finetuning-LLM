from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w3 = Window.orderBy("quantity", "id")
w_running = Window.partitionBy("product_id").orderBy("last_restock", "id")
w_grp = Window.partitionBy("product_id").orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("event_seq_2", "id")
w7 = Window.partitionBy("store_id").orderBy("quantity_rounded", "id")
w8 = Window.partitionBy("brand").orderBy(F.col("prev_prev_quantity_rounded").desc(), "id")
w_recent = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-05-17")))
    .select(
        "*",
        F.when(F.col("quantity") >= 231, "High").when(F.col("quantity") >= 98, "Medium").otherwise("Low").alias("quantity_band"),
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

enriched = (
    base
    .filter(F.col("store_id").isin([1, 2, 3, 5]))
    .select(
        "*",
        F.first("quantity_rounded", ignorenulls=True).over(w).alias("first_known_quantity_rounded"),
        F.lag("quantity_rounded", 1).over(w2).alias("prev_quantity_rounded"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w_running).alias("event_seq_2"),
        F.last("quantity_rounded", ignorenulls=True).over(w_grp).alias("last_known_quantity_rounded"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "product_id",
        "quantity",
        "quantity_rounded",
        "prev_quantity_rounded",
        "last_known_quantity_rounded",
        F.lag("prev_quantity_rounded", 1).over(w6).alias("prev_prev_quantity_rounded"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.product_id",
        "src.quantity",
        "src.prev_quantity_rounded",
        "src.prev_prev_quantity_rounded",
        "src.row_seq",
        F.col("b.cost"),
        F.col("b.launch_date"),
        F.col("b.brand"),
    )
)

deduped = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "*",
        F.sum("prev_prev_quantity_rounded").over(w_recent).alias("running_sum_prev_prev_quantity_rounded"),
        F.count("prev_prev_quantity_rounded").over(w_recent).alias("running_count_prev_prev_quantity_rounded"),
        F.min("prev_prev_quantity_rounded").over(w_recent).alias("running_min_prev_prev_quantity_rounded"),
        F.max("prev_prev_quantity_rounded").over(w_recent).alias("running_max_prev_prev_quantity_rounded"),
    )
)

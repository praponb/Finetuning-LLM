from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("store_id").orderBy("quantity_filled", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("store_id", "product_id").orderBy(F.col("adjusted_quantity").asc_nulls_first())
w_hist = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent_2 = Window.partitionBy("product_id").orderBy("adjusted_quantity", "id")
w5 = Window.partitionBy("store_id").orderBy("adjusted_quantity_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("next_adjusted_quantity_avg_5", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("store_id").orderBy("adjusted_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

valid = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5]))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
    .withColumn("adjusted_quantity", F.round((F.col("quantity") * 1.07), 2))
)

enriched = (
    valid
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity_filled",
        "adjusted_quantity",
        F.avg("adjusted_quantity").over(w_running).alias("adjusted_quantity_avg_5"),
        F.dense_rank().over(w_recent).alias("adjusted_quantity_dense_rank"),
        F.last("quantity", ignorenulls=True).over(w_hist).alias("last_known_quantity"),
        F.lag("quantity", 2).over(w_recent_2).alias("prev_quantity"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.lead("adjusted_quantity_dense_rank").over(w_recent_2).alias("next_adjusted_quantity_dense_rank"),
        F.lag("adjusted_quantity_dense_rank").over(w_recent_2).alias("prev_adjusted_quantity_dense_rank"),
        F.lead("adjusted_quantity_avg_5").over(w_recent_2).alias("next_adjusted_quantity_avg_5"),
        F.lag("adjusted_quantity_avg_5").over(w_recent_2).alias("prev_adjusted_quantity_avg_5"),
        F.row_number().over(w_recent_2).alias("event_seq"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "premium"))
    .select("e.*", F.col("p.cost"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.sum("quantity_filled").over(w5).alias("cumulative_quantity_filled"),
        F.avg("adjusted_quantity").over(w6).alias("adjusted_quantity_avg_2_prior"),
        F.sum("event_seq").over(w7).alias("cumulative_event_seq"),
    )
)

remaining_keys = (
    with_windows
    .select("product_id")
    .intersectAll(
        df_inventory
        .filter(F.col("quantity").isNull())
        .select("product_id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("prev_adjusted_quantity_avg_5") < 58)
    .select(
        "store_id",
        "product_id",
        "adjusted_quantity_avg_5",
        "adjusted_quantity_dense_rank",
        "last_known_quantity",
        "next_adjusted_quantity_dense_rank",
        "prev_adjusted_quantity_dense_rank",
        "next_adjusted_quantity_avg_5",
        "prev_adjusted_quantity_avg_5",
        "cost",
        "cumulative_quantity_filled",
        "cumulative_event_seq",
        F.col("adjusted_quantity_avg_2_prior").cast("string").alias("adjusted_quantity_avg_2_prior_num"),
    )
)

result = (
    cleaned
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "adjusted_quantity_avg_5",
        "adjusted_quantity_dense_rank",
        "last_known_quantity",
        "next_adjusted_quantity_dense_rank",
        "prev_adjusted_quantity_dense_rank",
        "next_adjusted_quantity_avg_5",
        "cost",
        "cumulative_quantity_filled",
        F.col("sum_inventory_quantity"),
    )
)

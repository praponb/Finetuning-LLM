from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id", "store_id").orderBy("quantity_filled", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("next_quantity_filled").desc())
w3 = Window.partitionBy("store_id").orderBy("prev_quantity")
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy("store_id").orderBy(F.col("next_quantity_filled_cume_dist").desc())
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.max("quantity").alias("max_inventory_quantity"))

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-03-01")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
)

metrics = (
    cleaned
    .filter(F.col("quantity_filled") < 38)
    .select(
        "id",
        "store_id",
        "quantity",
        "quantity_filled",
        "last_restock_label",
        F.lag("quantity_filled").over(w_hist).alias("prev_quantity_filled"),
        F.lead("quantity_filled").over(w_hist).alias("next_quantity_filled"),
        F.lag("quantity").over(w_hist).alias("prev_quantity"),
        F.lead("quantity").over(w_hist).alias("next_quantity"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

remaining_keys = (
    metrics
    .select("id", "store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("quantity") < 70)
        .select("id", "store_id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "store_id",
        "quantity",
        "last_restock_label",
        "next_quantity_filled",
        "next_quantity",
        "event_seq",
        F.cume_dist().over(w2).alias("next_quantity_filled_cume_dist"),
        F.percent_rank().over(w3).alias("prev_quantity_percent_rank"),
        F.max("next_quantity").over(w4).alias("store_id_max_next_quantity"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w5).alias("next_quantity_filled_cume_dist_cume_dist"),
        F.max("event_seq").over(w4).alias("store_id_max_event_seq"),
        F.min("next_quantity_filled_cume_dist").over(w4).alias("store_id_min_next_quantity_filled_cume_dist"),
        F.count("next_quantity_filled_cume_dist").over(w4).alias("store_id_count_next_quantity_filled_cume_dist"),
    )
)

kept_keys = (
    enriched
    .select("store_id")
    .subtract(
        df_inventory
        .filter(F.col("store_id") == 5)
        .select("store_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

report = (
    reconciled
    .select(
        "store_id",
        "next_quantity_filled",
        "next_quantity",
        "event_seq",
        "next_quantity_filled_cume_dist",
        "store_id_max_next_quantity",
        "next_quantity_filled_cume_dist_cume_dist",
        "store_id_min_next_quantity_filled_cume_dist",
        "store_id_count_next_quantity_filled_cume_dist",
    )
)

result = (
    report
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "next_quantity_filled",
        "next_quantity",
        "event_seq",
        "next_quantity_filled_cume_dist",
        "store_id_max_next_quantity",
        "next_quantity_filled_cume_dist_cume_dist",
        "store_id_min_next_quantity_filled_cume_dist",
        "store_id_count_next_quantity_filled_cume_dist",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("max_inventory_quantity").asc_nulls_first())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id")
w3 = Window.partitionBy("store_id").orderBy("last_restock", "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-03-16").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.round((F.col("quantity") * 0.9), 2).alias("quantity_adjusted"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "store_id",
        F.last("quantity_adjusted", ignorenulls=True).over(w).alias("last_known_quantity_adjusted"),
        F.avg("quantity_adjusted").over(w2).alias("store_id_avg_quantity_adjusted"),
        F.lag("quantity_adjusted", 1).over(w3).alias("prev_quantity_adjusted"),
    )
)

filtered = (
    enriched
    .filter(F.col("store_id_avg_quantity_adjusted") == 76)
    .select(
        "id",
        "store_id",
        "last_known_quantity_adjusted",
        F.least("store_id_avg_quantity_adjusted", "prev_quantity_adjusted").alias("least_store_id_avg_quantity_adjusted_prev_quantity_adjusted"),
        F.coalesce("store_id_avg_quantity_adjusted", F.lit(0)).alias("store_id_avg_quantity_adjusted_filled"),
    )
)

remaining_keys = (
    filtered
    .select("store_id", "id")
    .intersect(
        df_inventory
        .filter(F.col("product_id") == 3)
        .select("store_id", "id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("store_id_avg_quantity_adjusted_filled").between(56, 61))
    .select(
        "*",
        (F.col("store_id_avg_quantity_adjusted_filled") - F.col("least_store_id_avg_quantity_adjusted_prev_quantity_adjusted")).alias("store_id_avg_quantity_adjusted_filled_minus_least_store_id_avg_quantity_adjusted_prev_quantity_adjusted"),
        F.round("last_known_quantity_adjusted", 1).alias("last_known_quantity_adjusted_rounded"),
    )
)

result = (
    cleaned
    .groupBy(
        "store_id",
        F.when(F.col("store_id_avg_quantity_adjusted_filled_minus_least_store_id_avg_quantity_adjusted_prev_quantity_adjusted") >= 81, "LARGE").otherwise("SMALL").alias("store_id_avg_quantity_adjusted_filled_minus_least_store_id_avg_quantity_adjusted_prev_quantity_adjusted_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("store_id").asc_nulls_last())
)

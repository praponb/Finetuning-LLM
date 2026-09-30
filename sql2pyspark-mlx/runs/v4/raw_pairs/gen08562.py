from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_inventory
    .select(
        "*",
        F.first("quantity", ignorenulls=True).over(w_grp).alias("first_known_quantity"),
        F.sum("quantity").over(w_recent).alias("cumulative_quantity"),
    )
)

remaining_keys = (
    enriched
    .select("store_id", "product_id")
    .subtract(
        df_inventory
        .filter(F.col("quantity") < 206)
        .select("store_id", "product_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    reconciled
    .select("store_id", "product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("store_id") == 6)
        .select("store_id", "product_id")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("store_id").isin([2, 3, 5]))
    .select(
        "store_id",
        "product_id",
        "quantity",
        "first_known_quantity",
        "cumulative_quantity",
        F.col("cumulative_quantity").cast("double").alias("cumulative_quantity_num"),
    )
)

result = (
    valid
    .groupBy("product_id")
    .agg(
        F.round(F.avg("cumulative_quantity_num"), 2).alias("avg_cumulative_quantity_num"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("cumulative_quantity", [0.25, 0.5, 0.75]).alias("cumulative_quantity_quartiles"),
    )
    .orderBy(F.col("product_id").desc_nulls_last())
)

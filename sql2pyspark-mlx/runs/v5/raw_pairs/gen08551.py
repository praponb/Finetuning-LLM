from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("is_new_york", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_grp = Window.orderBy("inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("sqft_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("last4_count_sqft")
w_running = Window.orderBy("inventory_last_restock", "id")

cleaned = (
    df_stores
    .filter(~F.col("region").isin(["South", "West", "Central"]))
    .select(
        "*",
        F.round("sqft", 1).alias("sqft_rounded"),
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "opened_date",
        "sqft_rounded",
        "resolved_region",
        F.count("sqft").over(w).alias("last4_count_sqft"),
        F.min("sqft").over(w).alias("last4_min_sqft"),
        F.max("sqft").over(w).alias("last4_max_sqft"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.opened_date")), "left_semi")
)

filtered = (
    kept
    .filter(F.col("cumulative_sqft") >= 64070)
    .select(
        "id",
        "sqft_rounded",
        "resolved_region",
        "last4_count_sqft",
        "cumulative_sqft",
        "inventory_quantity",
        "inventory_last_restock",
        F.col("last4_max_sqft").cast("int").alias("last4_max_sqft_int"),
    )
)

result = (
    filtered
    .select(
        "id",
        "sqft_rounded",
        "resolved_region",
        "inventory_quantity",
        "inventory_last_restock",
        F.last("cumulative_sqft", ignorenulls=True).over(w_grp).alias("last_known_cumulative_sqft"),
        F.sum("sqft_rounded").over(w5).alias("running_sqft_rounded"),
        F.rank().over(w6).alias("last4_count_sqft_rank"),
        F.lead("cumulative_sqft", 2).over(w_running).alias("next_cumulative_sqft"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.orderBy("quantity")
w4 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy(F.col("last_known_quantity_2").desc_nulls_first())
w7 = Window.partitionBy("product_id")
w8 = Window.partitionBy("product_id").orderBy(F.col("last_known_quantity_2").desc_nulls_last())

metrics = (
    df_inventory
    .select(
        "*",
        F.ntile(5).over(w).alias("quantity_bucket"),
        F.ntile(3).over(w2).alias("quantity_bucket_2"),
        F.percent_rank().over(w3).alias("quantity_percent_rank"),
        F.last("quantity", ignorenulls=True).over(w4).alias("last_known_quantity"),
        F.last("quantity", ignorenulls=True).over(w5).alias("last_known_quantity_2"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.region"), F.col("s.sqft"))
)

remaining_keys = (
    enriched
    .select("product_id", "store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("quantity").between(71, 119))
        .select("product_id", "store_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-04-13")))
    .select(
        "id",
        "product_id",
        "quantity",
        "quantity_bucket",
        "quantity_bucket_2",
        "quantity_percent_rank",
        "last_known_quantity",
        "last_known_quantity_2",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

with_windows = (
    filtered
    .withColumn("last_known_quantity_2_dense_rank", F.dense_rank().over(w6))
    .withColumn("product_id_avg_last_known_quantity", F.avg("last_known_quantity").over(w7))
    .withColumn("last_known_quantity_2_percent_rank", F.percent_rank().over(w6))
)

matched_keys = (
    with_windows
    .select("id", "product_id")
    .subtract(
        df_inventory
        .filter(F.col("product_id") == 5)
        .select("id", "product_id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched_inventory = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w8).alias("last_known_quantity_2_dense_rank_2"),
        F.cume_dist().over(w3).alias("quantity_cume_dist"),
        F.min("product_id_avg_last_known_quantity").over(w7).alias("product_id_min_product_id_avg_last_known_quantity"),
    )
)

result = (
    enriched_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "quantity_bucket",
        "quantity_percent_rank",
        "last_known_quantity",
        "resolved_region",
        "last_known_quantity_2_dense_rank",
        "last_known_quantity_2_percent_rank",
    )
    .orderBy("product_id", "last_known_quantity_2_dense_rank")
)

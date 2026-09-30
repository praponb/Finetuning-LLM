from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity")
w_recent = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w3 = Window.orderBy(F.col("quantity_percent_rank").desc_nulls_last())
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

valid = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4, 6]))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 260, "High").when(F.col("quantity") >= 31, "Medium").otherwise("Low"))
)

kept_keys = (
    valid
    .select("id", "store_id")
    .intersect(
        df_inventory
        .filter(F.col("store_id").isin([1, 5]))
        .select("id", "store_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("last_restock") >= F.lit("2023-07-13").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_tier",
        F.when(F.col("quantity") >= 183, "High").when(F.col("quantity") >= 113, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_tier",
        "quantity_level",
        F.percent_rank().over(w).alias("quantity_percent_rank"),
        F.cume_dist().over(w_recent).alias("quantity_cume_dist"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "store_id",
        "quantity",
        "quantity_tier",
        "quantity_level",
        "quantity_percent_rank",
        F.cume_dist().over(w3).alias("quantity_percent_rank_cume_dist"),
        F.percent_rank().over(w).alias("quantity_percent_rank_2"),
    )
)

valid_inventory = (
    with_windows
    .filter(F.col("quantity") > 222)
    .withColumn("quantity_percent_rank_2_plus_quantity_percent_rank_cume_dist", (F.col("quantity_percent_rank_2") + F.col("quantity_percent_rank_cume_dist")))
)

kept_keys_2 = (
    valid_inventory
    .select("store_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.year("last_restock") == 2025)
        .select("store_id", "id")
    )
)

kept_rows = (
    valid_inventory
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity",
        "quantity_level",
        "quantity_percent_rank",
        "quantity_percent_rank_2",
        "quantity_percent_rank_2_plus_quantity_percent_rank_cume_dist",
        F.col("avg_inventory_quantity"),
    )
    .orderBy("avg_inventory_quantity")
)

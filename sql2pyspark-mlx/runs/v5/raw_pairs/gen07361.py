from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("store_id").orderBy(F.col("store_id_sum_quantity").desc_nulls_first())
w4 = Window.orderBy(F.col("quantity").desc())
w5 = Window.partitionBy("store_region", "store_id")
w6 = Window.partitionBy("city", "store_id")

cleaned = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 4]))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.date_trunc("WEEK", "last_restock").alias("last_restock_period"),
    )
)

remaining_keys = (
    cleaned
    .select("id", "store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id").isin([2, 3, 4, 6]))
        .select("id", "store_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .filter(F.col("last_restock") < "2025-01-04")
    .select("*", F.sum("quantity").over(w).alias("store_id_sum_quantity"))
)

joined = (
    metrics
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.opened_date"), F.col("s.store_name"), F.col("s.region"))
)

joined_inventory = (
    joined
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select(
        "a.*",
        F.col("d.store_name").alias("store_store_name"),
        F.col("d.city"),
        F.col("d.region").alias("store_region"),
    )
)

metrics_inventory = (
    joined_inventory
    .select(
        "*",
        F.max("store_id_sum_quantity").over(w2).alias("region_max_store_id_sum_quantity"),
        F.dense_rank().over(w3).alias("store_id_sum_quantity_dense_rank"),
    )
)

metrics_inventory_2 = metrics_inventory.withColumn("quantity_rank", F.rank().over(w4))

result = (
    metrics_inventory_2
    .select(
        "last_restock",
        "store_id_sum_quantity",
        "opened_date",
        "store_store_name",
        "city",
        "region_max_store_id_sum_quantity",
        F.round((F.col("quantity_rank") / F.sum("quantity_rank").over(w5)), 4).alias("quantity_rank_share"),
        F.max("quantity_rank").over(w6).alias("city_max_quantity_rank"),
        F.avg("quantity_rank").over(w).alias("store_id_avg_quantity_rank"),
    )
)

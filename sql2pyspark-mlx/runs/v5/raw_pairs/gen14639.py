from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("sqft").asc_nulls_first())
w2 = Window.partitionBy("city", "store_id").orderBy(F.col("store_sqft").desc_nulls_last())

with_dims = (
    df_inventory
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.last_restock",
        F.col("s.store_name"),
        F.col("s.sqft"),
    )
)

enriched = (
    with_dims
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select(
        "s.*",
        F.col("p2.opened_date"),
        F.col("p2.sqft").alias("store_sqft"),
        F.col("p2.store_name").alias("store_store_name"),
    )
)

enriched_inventory = (
    enriched
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.city"))
)

filtered = (
    enriched_inventory
    .filter(F.col("city").isNotNull() & (F.col("sqft") == 7072))
    .select("*", F.lower("store_name").alias("store_name_lower"))
)

remaining_keys = (
    filtered
    .select("store_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("quantity").isNull())
        .select("store_id", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .filter(F.col("store_id").isin([2, 3, 4, 5]))
    .select("*", F.cume_dist().over(w_ordered).alias("sqft_cume_dist"))
)

metrics = with_windows.withColumn("store_sqft_rank", F.rank().over(w2))

result = (
    metrics
    .select(
        "id",
        "store_id",
        "store_name",
        "sqft",
        "store_sqft",
        "store_store_name",
        "city",
        "store_name_lower",
        "sqft_cume_dist",
    )
    .orderBy(F.col("id").desc_nulls_first())
)

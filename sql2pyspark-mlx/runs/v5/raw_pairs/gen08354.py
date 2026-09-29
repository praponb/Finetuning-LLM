from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("product_id")
w_running = Window.partitionBy("store_id", "product_id")
w_recent = Window.partitionBy("product_id", "store_id").orderBy(F.col("store_id_count_quantity").desc(), "id")

filtered = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 1))
)

scored = (
    filtered
    .withColumn("running_quantity", F.sum("quantity").over(w))
    .withColumn("next_quantity", F.lead("quantity", 1).over(w2))
    .withColumn("store_id_count_quantity", F.count("quantity").over(w3))
)

with_windows = (
    scored
    .select(
        "id",
        "store_id",
        "product_id",
        "running_quantity",
        F.avg("store_id_count_quantity").over(w4).alias("product_id_avg_store_id_count_quantity"),
        F.max("running_quantity").over(w_running).alias("store_id_max_running_quantity"),
        F.ntile(3).over(w_recent).alias("store_id_count_quantity_bucket"),
    )
)

enriched = (
    with_windows
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.sqft") != 4837)
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.running_quantity",
        "e.store_id_max_running_quantity",
        "e.store_id_count_quantity_bucket",
        F.col("p.opened_date"),
        F.col("p.region"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select(
        "e.*",
        F.col("s.store_name"),
        F.col("s.opened_date").alias("store_opened_date"),
        F.col("s.city"),
    )
)

remaining_keys = (
    with_dims
    .select("id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("store_id") == 2)
        .select("id", "store_id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

with_dims_inventory = (
    verified
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.price"))
)

result = (
    with_dims_inventory
    .groupBy("store_id", "city")
    .pivot("region", ["Central", "East"])
    .agg(F.sum("price"))
    .select(
        "store_id",
        "city",
        F.col("Central").alias("central"),
        F.col("East").alias("east"),
    )
)

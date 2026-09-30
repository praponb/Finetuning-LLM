from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.orderBy("quantity")

cleaned = (
    df_inventory
    .filter(F.col("product_id") == 9)
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
        F.year("last_restock").alias("last_restock_year"),
    )
)

kept_keys = (
    cleaned
    .select("store_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("quantity") < 228)
        .select("store_id", "id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .filter(F.col("quantity").between(118, 195))
    .select(
        "id",
        "store_id",
        "last_restock",
        "last_restock_year",
        F.stddev_samp("quantity").over(w).alias("store_id_stddev_samp_quantity"),
        F.stddev_samp("last_restock_year").over(w).alias("store_id_stddev_samp_last_restock_year"),
        F.max("quantity").over(w).alias("store_id_max_quantity"),
        F.percent_rank().over(w2).alias("quantity_percent_rank"),
    )
)

joined = (
    enriched
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select(
        "e.store_id",
        "e.last_restock_year",
        "e.store_id_stddev_samp_quantity",
        "e.store_id_max_quantity",
        F.col("s.city"),
        F.col("s.region"),
        F.col("s.store_name"),
    )
)

result = (
    joined
    .groupBy("store_id")
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.max(F.when(F.col("region") == "North", F.col("last_restock_year"))).alias("north_max_last_restock_year"),
        F.percentile_approx("store_id_stddev_samp_quantity", [0.25, 0.5, 0.75]).alias("store_id_stddev_samp_quantity_quartiles"),
        F.round(F.sum("store_id_max_quantity") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("store_id_max_quantity_per_berlin"),
        F.percentile_approx("store_id_stddev_samp_quantity", [0.25, 0.5, 0.75]).alias("store_id_stddev_samp_quantity_quartiles_2"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("store_id_max_quantity_per_berlin").desc_nulls_last())
)

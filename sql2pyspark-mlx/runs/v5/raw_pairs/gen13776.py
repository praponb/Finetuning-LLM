from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock_dayofweek").asc_nulls_last())
w3 = Window.partitionBy("product_id").orderBy("last_restock_dayofweek", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_first())
w5 = Window.partitionBy("city").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("store_sqft").desc_nulls_last(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= "2025-06-01")
    .select("*", F.dayofweek("last_restock").alias("last_restock_dayofweek"))
)

enriched = (
    valid
    .select(
        "id",
        "store_id",
        "quantity",
        F.lead("last_restock_dayofweek", 2).over(w).alias("next_last_restock_dayofweek"),
        F.rank().over(w2).alias("last_restock_dayofweek_rank"),
        F.row_number().over(w3).alias("row_seq"),
        F.cume_dist().over(w4).alias("quantity_cume_dist"),
    )
)

enriched_inventory = (
    enriched
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.quantity",
        "a.row_seq",
        "a.quantity_cume_dist",
        F.col("d.opened_date"),
        F.col("d.city"),
    )
)

latest = (
    enriched_inventory
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

joined = (
    latest
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.store_name").isNull())
    .select(
        "s.id",
        "s.store_id",
        "s.quantity",
        "s.city",
        F.col("b.opened_date").alias("store_opened_date"),
        F.col("b.sqft"),
        F.col("b.store_name"),
    )
)

with_dims = (
    joined
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.store_opened_date",
        "a.store_name",
        F.col("s.sqft").alias("store_sqft"),
    )
)

latest_inventory = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest_inventory
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.region").alias("region"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.store_sqft")).alias("max_store_sqft"),
        F.grouping_id().alias("grouping_level"),
    )
)

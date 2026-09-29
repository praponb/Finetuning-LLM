from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("region").orderBy("quantity_num", "id")
w3 = Window.partitionBy("store_id").orderBy("last_restock_plus_11d", "id")

filtered = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 6]))
    .select(
        "*",
        F.col("quantity").cast("double").alias("quantity_num"),
        F.date_add("last_restock", 11).alias("last_restock_plus_11d"),
    )
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_dims = (
    latest
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.store_id",
        "e.last_restock",
        "e.quantity_num",
        "e.last_restock_plus_11d",
        F.col("d.region"),
    )
)

joined = (
    with_dims
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.last_restock",
        "e.quantity_num",
        "e.last_restock_plus_11d",
        "e.region",
        F.col("s.region").alias("store_region"),
    )
)

enriched = (
    joined
    .select(
        "*",
        F.ntile(5).over(w2).alias("quantity_num_bucket"),
        F.lead("quantity_num", 1).over(w3).alias("next_quantity_num"),
    )
)

result = (
    enriched
    .withColumn("next_quantity_num_tier", F.when(F.col("next_quantity_num") >= 63, "LARGE").otherwise("SMALL"))
    .cube("next_quantity_num_tier")
    .agg(
        F.round(F.avg("quantity_num_bucket"), 2).alias("avg_quantity_num_bucket"),
        F.sum(F.when(F.col("region") == "Central", F.col("next_quantity_num")).otherwise(0)).alias("central_next_quantity_num"),
        F.avg(F.when(F.col("region") == "East", F.col("next_quantity_num"))).alias("east_avg_next_quantity_num"),
        F.countDistinct("store_region").alias("distinct_store_regions"),
        F.grouping("next_quantity_num_tier").alias("grouping_id"),
    )
    .filter(F.col("avg_quantity_num_bucket") > 0)
    .orderBy(F.col("east_avg_next_quantity_num").asc_nulls_last())
)

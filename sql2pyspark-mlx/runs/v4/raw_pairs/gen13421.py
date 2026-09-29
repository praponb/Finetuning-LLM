from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc_nulls_first())
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w3 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("quantity_cume_dist").desc_nulls_first(), "id")

enriched = (
    df_inventory
    .withColumn("quantity_cume_dist", F.cume_dist().over(w))
    .withColumn("quantity_rank", F.rank().over(w2))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w3))
    .withColumn("cumulative_quantity_2", F.sum("quantity").over(w4))
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

remaining_keys = (
    latest
    .select("store_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") > "2024-11-01")
        .select("store_id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.opened_date"), F.col("p.region"), F.col("p.sqft"))
)

joined = (
    with_dims
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.stock_qty"), F.col("p.product_name"))
)

daily = (
    joined
    .groupBy(
        F.when(F.col("quantity_cume_dist") >= 0.6, "LARGE").otherwise("SMALL").alias("quantity_cume_dist_tier"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
        "region",
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_count"),
    )
    .filter(F.col("num_regions") < 1)
)

result = (
    daily
    .select(
        "quantity_cume_dist_tier",
        "year_start",
        "region",
        "region_list",
        "num_regions",
        "north_count",
    )
    .orderBy(F.col("region").desc())
)

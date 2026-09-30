from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("inventory_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "region").orderBy("inventory_quantity", "id")
w4 = Window.partitionBy("city").orderBy(F.col("inventory_quantity").desc(), "id")
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("city", "region")
w7 = Window.partitionBy("region").orderBy("inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("city", "region").orderBy("latest_rank", "id").rowsBetween(-5, -1)

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-02-25").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.round((F.col("sqft") * 0.85), 2).alias("adjusted_sqft"),
        F.length("region").alias("region_length"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.opened_date")), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

enriched = (
    with_last
    .withColumn("running_count_inventory_quantity", F.count("inventory_quantity").over(w2))
    .withColumn("running_max_inventory_quantity", F.max("inventory_quantity").over(w2))
    .withColumn("running_min_inventory_quantity", F.min("inventory_quantity").over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "inventory_last_restock",
        "inventory_quantity",
        "running_count_inventory_quantity",
        "running_max_inventory_quantity",
        "running_min_inventory_quantity",
        "event_seq",
        "latest_rank",
        F.sum("running_min_inventory_quantity").over(w5).alias("city_sum_running_min_inventory_quantity"),
        F.avg("running_count_inventory_quantity").over(w6).alias("city_avg_running_count_inventory_quantity"),
        F.sum("inventory_quantity").over(w7).alias("running_inventory_quantity"),
        F.avg("inventory_quantity").over(w8).alias("inventory_quantity_avg_5_prior"),
    )
)

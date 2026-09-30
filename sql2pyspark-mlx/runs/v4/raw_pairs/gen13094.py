from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("region", "city").orderBy("sqft", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

ranked = (
    df_stores
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.ntile(5).over(w2).alias("sqft_bucket"),
        F.lag("sqft", 2).over(w3).alias("prev_sqft"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.opened_date")), "left_semi")
)

result = (
    not_flagged
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("region") == "South", F.col("inventory_quantity"))).alias("south_total_inventory_quantity"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.max(F.when(F.col("region") == "South", F.col("inventory_quantity"))).alias("south_max_inventory_quantity"),
    )
    .filter(F.col("south_total_inventory_quantity") > 0)
)

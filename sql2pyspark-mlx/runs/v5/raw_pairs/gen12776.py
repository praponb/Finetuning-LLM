from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("region").orderBy("sqft", "id")
w_recent = Window.partitionBy("region", "status").orderBy("sqft", "id")
w6 = Window.partitionBy("status").orderBy(F.col("opened_date").desc_nulls_first())
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

latest = (
    df_stores
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "store_name", "region", "opened_date", "sqft")
)

with_last = (
    latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"))
)

enriched = (
    with_last
    .filter(F.col("region") == "South")
    .select(
        "*",
        F.ntile(10).over(w_hist).alias("sqft_bucket"),
        F.lead("sqft", 1, 0).over(w4).alias("next_sqft"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

with_last_stores = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

wide = (
    with_last_stores
    .groupBy("region")
    .pivot("status", ["COMPLETED", "PENDING", "CANCELLED"])
    .agg(F.max("next_sqft"))
)

result = wide.select("region", "PENDING", "CANCELLED")

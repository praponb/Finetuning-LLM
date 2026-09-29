from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("region").orderBy("order_order_date", "id")
w_recent = Window.partitionBy("region")

cleaned = (
    df_stores
    .filter(F.col("city").isin(["Singapore", "New York"]))
    .select("*", F.initcap(F.trim("region")).alias("region_title"))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.region",
        "s.opened_date",
        "s.region_title",
        F.col("c.order_date").alias("order_order_date"),
        F.col("c.shipping_fee"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.region_title",
        "s.order_order_date",
        "s.shipping_fee",
        F.col("c.quantity"),
        F.col("c.last_restock"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.order_order_date")), "left_anti")
)

scored = (
    not_flagged
    .select(
        "id",
        "shipping_fee",
        "last_restock",
        F.lag("quantity", 1, 0).over(w3).alias("prev_quantity"),
        F.min("quantity").over(w_recent).alias("region_min_quantity"),
    )
)

result = (
    scored
    .select(
        "id",
        "shipping_fee",
        "last_restock",
        "prev_quantity",
        "region_min_quantity",
    )
)

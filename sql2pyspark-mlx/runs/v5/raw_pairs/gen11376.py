from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("adjusted_unit_price").desc(), "id")
w2 = Window.orderBy("quantity", "id")
w3 = Window.orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "quantity",
        F.round((F.col("unit_price") * 1.1), 2).alias("adjusted_unit_price"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.id", "e.order_id", "e.quantity", "e.dedup_rank", F.col("d.channel"))
)

enriched_order_items = (
    enriched
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.lag("quantity", 1).over(w2).alias("prev_quantity"),
        F.sum("quantity").over(w3).alias("running_quantity"),
    )
)

result = (
    enriched_order_items
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.channel").alias("order_channel"),
        F.col("d.status").alias("status"),
        F.col("e.channel").alias("order_item_channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.prev_quantity")).alias("min_prev_quantity"),
        F.min(F.col("e.quantity")).alias("min_quantity"),
    )
)

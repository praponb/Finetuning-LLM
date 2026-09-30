from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_filled").asc_nulls_last(), "id")
w2 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity_filled").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("quantity_filled").desc_nulls_first(), "id")
w_hist = Window.partitionBy("order_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("quantity_filled", "id").rowsBetween(-2, Window.currentRow)
unit_price_by_product_id = df_order_items.groupBy("product_id").agg(F.max("unit_price").alias("max_order_item_unit_price"))

cleaned = (
    df_order_items
    .filter(F.col("product_id").isin([3, 4, 37]))
    .select(
        "id",
        "order_id",
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "product_id", "quantity_filled")
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked_order_items = (
    ranked
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

with_dims = (
    ranked_order_items
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.channel"), F.col("b.discount"))
)

with_windows = (
    with_dims
    .filter(F.col("channel") == "web")
    .select(
        "id",
        "product_id",
        "channel",
        "discount",
        F.first("discount", ignorenulls=True).over(w_hist).alias("first_known_discount"),
        F.avg("quantity_filled").over(w5).alias("quantity_filled_avg_2"),
    )
)

result = (
    with_windows
    .join(unit_price_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "channel",
        "first_known_discount",
        "quantity_filled_avg_2",
        F.when(F.col("channel").rlike("^(store|mobile)$"), "GROUP_B").when(F.col("channel").rlike("^(web|partner)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("channel_zone"),
        F.col("max_order_item_unit_price"),
    )
    .orderBy(F.col("max_order_item_unit_price").desc_nulls_first(), "product_id")
)

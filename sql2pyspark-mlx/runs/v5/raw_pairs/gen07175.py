from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("discount").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price_adjusted").desc_nulls_last())
w3 = Window.partitionBy("order_channel").orderBy("unit_price_adjusted", "id")

joined = (
    df_order_items
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id").eqNullSafe(F.col("d.id")), "inner")
    .filter(F.month(F.col("d.launch_date")).isin([3, 10, 11]))
    .select("s.*", F.col("d.launch_date"), F.col("d.category"))
)

joined_order_items = (
    joined
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.unit_price",
        "e.category",
        F.col("b.channel"),
        F.col("b.amount"),
        F.col("b.discount"),
    )
)

cleaned = (
    joined_order_items
    .filter(F.col("category").isNotNull())
    .select(
        "*",
        F.lower("channel").alias("channel_lower"),
        F.round((F.col("unit_price") * 0.9), 2).alias("unit_price_adjusted"),
    )
)

deduped = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    deduped
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.channel").alias("order_channel"))
)

metrics = (
    enriched
    .select(
        "id",
        "unit_price",
        "channel",
        "amount",
        "order_channel",
        F.cume_dist().over(w2).alias("unit_price_adjusted_cume_dist"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "channel",
        "amount",
        "order_channel",
        "unit_price_adjusted_cume_dist",
        "seq_no",
    )
    .orderBy(F.col("channel").desc_nulls_first(), F.col("id").desc())
)

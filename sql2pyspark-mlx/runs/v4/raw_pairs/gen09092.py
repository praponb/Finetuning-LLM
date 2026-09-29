from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("unit_price")
w2 = Window.partitionBy("channel", "product_id").orderBy(F.col("amount").asc_nulls_first(), "id")
w3 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 2, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

with_dims = (
    filtered
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.amount"), F.col("b.discount"), F.col("b.channel"))
)

ranked = (
    with_dims
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

cleaned = (
    ranked
    .filter(F.col("product_id").isin([13, 39]))
    .withColumn("unit_price_rounded", F.round("unit_price", 0))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("NONE")))
)

ranked_order_items = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked_order_items
    .select(
        "*",
        F.sum("discount").over(w3).alias("cumulative_discount"),
        F.sum("discount").over(w4).alias("cumulative_discount_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "product_id",
        "discount",
        "unit_price",
        "amount",
        "unit_price_rounded",
    )
    .unpivot(
        ["id", "product_id"],
        ["discount", "unit_price", "amount", "unit_price_rounded"],
        "metric",
        "val",
    )
    .filter(F.col("val").isNotNull())
)

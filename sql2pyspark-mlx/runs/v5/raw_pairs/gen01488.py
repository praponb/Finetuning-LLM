from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("channel").orderBy(F.col("discount").asc_nulls_last(), "id")
w3 = Window.partitionBy("status").orderBy("amount", "id")
w4 = Window.partitionBy("channel")

valid = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-03-21").cast("date"))
    .withColumn("shipping_fee_filled", F.coalesce("shipping_fee", F.lit(0)))
    .withColumn("discount_per_shipping_fee", (F.col("discount") / F.nullif("shipping_fee", F.lit(0))))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id").isin([5, 20])), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.order_date",
        "s.status",
        "s.channel",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        "s.shipping_fee_filled",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.delivered_date") < F.col("s.order_date")), "left_semi")
)

ranked = (
    eligible
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    ranked
    .filter(F.col("order_item_quantity") > 5)
    .withColumn("prev_order_item_quantity", F.lag("order_item_quantity", 1).over(w3))
    .withColumn("channel_max_order_item_quantity", F.max("order_item_quantity").over(w4))
)

result = (
    scored
    .select(
        "id",
        "channel",
        F.col("discount").alias("DISCOUNT"),
        F.col("shipping_fee_filled").alias("SHIPPING_FEE_FILLED"),
        F.col("shipping_fee").alias("SHIPPING_FEE"),
        F.col("amount").alias("AMOUNT"),
    )
    .unpivot(
        ids=["id", "channel"],
        values=["DISCOUNT", "SHIPPING_FEE_FILLED", "SHIPPING_FEE", "AMOUNT"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)

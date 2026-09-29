from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter(F.col("status").isin(["SHIPPED", "RETURNED", "COMPLETED"]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        F.round((F.col("shipping_fee") * 1.1), 2).alias("adjusted_shipping_fee"),
        F.initcap(F.trim("channel")).alias("channel_title"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

filtered = (
    latest_match
    .filter(F.col("adjusted_shipping_fee") <= 63)
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "adjusted_shipping_fee",
        "channel_title",
        "order_item_unit_price",
        F.ceil("adjusted_shipping_fee").alias("adjusted_shipping_fee_whole"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

result = (
    filtered
    .select(
        "id",
        "store_id",
        F.col("adjusted_shipping_fee").alias("ADJUSTED_SHIPPING_FEE"),
        "amount",
        F.col("order_item_unit_price").alias("ORDER_ITEM_UNIT_PRICE_VAL"),
    )
    .unpivot(
        ["id", "store_id"],
        ["ADJUSTED_SHIPPING_FEE", "amount", "ORDER_ITEM_UNIT_PRICE_VAL"],
        "metric",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

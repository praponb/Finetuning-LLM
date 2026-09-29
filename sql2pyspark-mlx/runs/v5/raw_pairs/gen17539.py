from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy("order_date")

base = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-09-06"))) & F.col("store_id").isin([27, 33, 39]))
    .withColumn("discount_plus_shipping_fee", (F.col("discount") + F.col("shipping_fee")))
)

latest_match = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.channel",
        "s.discount",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.product_id").isNotNull()), "left_semi")
)

deduped = (
    not_flagged
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "discount",
        "order_item_unit_price",
        "order_item_quantity",
    )
)

result = (
    deduped
    .groupBy("channel")
    .pivot("status", ["RETURNED", "CANCELLED", "SHIPPED"])
    .agg(F.max("discount").alias("value_sum"))
)

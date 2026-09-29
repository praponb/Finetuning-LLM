from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.orderBy("order_date", "id")
w3 = Window.partitionBy("status", "customer_id").orderBy("shipping_fee", "id")
w_grp_2 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("shipment_status", "status").orderBy(F.col("next_discount").desc_nulls_last())
w7 = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w8 = Window.partitionBy("status", "shipment_status").orderBy("order_date", "id")
w_part = Window.orderBy(F.col("next_discount").desc(), "id")
w10 = Window.partitionBy("status", "channel").orderBy("shipping_fee", "id")

metrics = (
    df_orders
    .withColumn("amount_bucket", F.ntile(3).over(w_grp))
    .withColumn("next_discount", F.lead("discount", 1).over(w2))
    .withColumn("shipping_fee_bucket", F.ntile(4).over(w3))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w_grp_2))
)

latest_match = (
    metrics
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.channel",
        "s.amount",
        "s.shipping_fee",
        "s.next_discount",
        "s.last_known_channel",
        F.col("c.status").alias("shipment_status"),
    )
)

enriched = (
    latest_match
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        "shipment_status",
        F.dense_rank().over(w6).alias("next_discount_dense_rank"),
        F.lag("next_discount", 1).over(w7).alias("prev_next_discount"),
        F.lag("shipping_fee", 2).over(w8).alias("prev_shipping_fee"),
        F.row_number().over(w_part).alias("row_seq"),
        F.lead("amount", 2).over(w10).alias("next_amount"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.month(F.col("x.shipped_date")).isin([1, 4, 6])), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id").isin([13, 34, 37, 39])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        "shipment_status",
        "prev_next_discount",
        "row_seq",
        "next_amount",
    )
)

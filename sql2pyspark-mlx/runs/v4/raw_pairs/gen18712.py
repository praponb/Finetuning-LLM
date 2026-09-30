from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel", "status")
w4 = Window.partitionBy("store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

cleaned = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.trim("status").alias("clean_status"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") >= F.col("s.order_date")), "left_anti")
)

enriched = (
    kept
    .select(
        "*",
        F.row_number().over(w_hist).alias("event_seq"),
        F.sum("amount").over(w2).alias("running_amount"),
        F.corr("discount", "amount").over(w3).alias("corr_discount_amount"),
        F.sum("shipping_fee").over(w4).alias("running_shipping_fee"),
    )
)

kept_orders = (
    enriched
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "left_anti")
)

with_latest = (
    kept_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.delivered_date").alias("shipment_delivered_date"))
)

result = (
    with_latest
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("event_seq")).otherwise(0)).alias("completed_event_seq"),
    )
    .filter(F.col("mobile_rows") < 1)
)

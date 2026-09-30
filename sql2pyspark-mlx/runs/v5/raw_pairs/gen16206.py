from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("customer_id", "shipment_status").orderBy("delivered_date", "id")
w4 = Window.partitionBy("shipment_status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

with_latest = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("shipment_status"))
)

filtered = (
    with_latest
    .filter(F.col("order_date") > F.lit("2023-06-05").cast("date"))
    .select(
        "*",
        F.round("amount", 0).alias("amount_rounded"),
        F.initcap(F.trim("channel")).alias("channel_title"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.discount",
        "s.shipment_status",
        "s.channel_title",
        F.col("c.delivered_date"),
    )
)

with_windows = (
    latest_match
    .filter(F.col("discount") > 0.36)
    .select(
        "*",
        F.lead("discount").over(w3).alias("next_discount"),
        F.lag("discount").over(w3).alias("prev_discount"),
        F.row_number().over(w3).alias("seq_no"),
        F.sum("discount").over(w4).alias("cumulative_discount"),
    )
)

valid = (
    with_windows
    .filter(F.col("cumulative_discount") > 1)
    .select(
        "*",
        (F.col("prev_discount") * F.col("discount")).alias("prev_discount_times_discount"),
        F.coalesce("shipment_status", F.lit("UNKNOWN")).alias("shipment_status_filled"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.delivered_date") < F.col("s.delivered_date")), "left_anti")
)

result = (
    kept
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "discount",
        "shipment_status",
        "channel_title",
        "next_discount",
        "prev_discount",
        "seq_no",
        F.col("max_order_amount"),
    )
    .orderBy(F.col("discount").desc_nulls_first())
)

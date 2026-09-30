from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("discount", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())
w4 = Window.orderBy("cumulative_amount_rounded", "id")
w5 = Window.partitionBy("shipment_carrier", "shipment_status")
w_hist = Window.orderBy("amount_rounded")
w7 = Window.partitionBy("shipment_status").orderBy("amount", "id").rowsBetween(-3, -1)
w_grp = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("shipment_status").orderBy(F.col("amount_rounded_percent_rank").desc_nulls_first(), "id")

valid = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-08-18"))) & (F.col("channel").eqNullSafe(F.lit("store"))))
    .withColumn("amount_rounded", F.round("amount", 2))
)

with_windows = (
    valid
    .withColumn("cumulative_amount_rounded", F.sum("amount_rounded").over(w))
    .withColumn("next_shipping_fee", F.lead("shipping_fee", 1).over(w2))
)

with_last = (
    with_windows
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.status",
        "s.amount",
        "s.shipping_fee",
        "s.amount_rounded",
        "s.cumulative_amount_rounded",
        "s.next_shipping_fee",
        F.col("c.carrier").alias("shipment_carrier"),
        F.col("c.status").alias("shipment_status"),
    )
)

with_windows_orders = (
    with_last
    .filter(F.col("amount") <= 740)
    .select(
        "id",
        "shipping_fee",
        "amount_rounded",
        "cumulative_amount_rounded",
        "next_shipping_fee",
        "shipment_status",
        F.lead("cumulative_amount_rounded", 2).over(w4).alias("next_cumulative_amount_rounded"),
        F.stddev_samp("amount").over(w5).alias("shipment_carrier_stddev_samp_amount"),
        F.percent_rank().over(w_hist).alias("amount_rounded_percent_rank"),
        F.avg("amount_rounded").over(w7).alias("amount_rounded_avg_3_prior"),
        F.sum("next_shipping_fee").over(w_grp).alias("cumulative_next_shipping_fee"),
    )
)

not_flagged = (
    with_windows_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < "2023-03-16"), "left_anti")
)

latest = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

stats = (
    latest
    .groupBy(
        "shipment_status",
        F.when(F.col("shipping_fee") >= 44, "PREMIUM").otherwise("STANDARD").alias("shipping_fee_tier"),
    )
    .agg(
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.sort_array(F.collect_set("shipment_status")).alias("shipment_status_list"),
        F.size(F.collect_set("shipment_status")).alias("num_shipment_status"),
        F.round(F.sum("amount_rounded") / F.nullif(F.count(F.when(F.col("shipment_status") == "IN_TRANSIT", 1)), F.lit(0)), 2).alias("amount_rounded_per_in_transit"),
        F.sum(F.when(F.col("shipment_status") == "DELIVERED", F.col("next_cumulative_amount_rounded")).otherwise(0)).alias("delivered_next_cumulative_amount_rounded"),
    )
    .filter(F.col("num_shipment_status") > 1)
)

result = (
    stats
    .select(
        "shipment_status",
        "shipping_fee_tier",
        "shipping_fee_stddev",
        "shipment_status_list",
        "delivered_next_cumulative_amount_rounded",
    )
    .orderBy(F.col("delivered_next_cumulative_amount_rounded").desc())
)

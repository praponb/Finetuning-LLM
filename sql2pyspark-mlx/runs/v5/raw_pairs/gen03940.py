from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status", "channel").orderBy(F.col("shipping_fee").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("discount", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("channel").orderBy(F.col("shipping_fee_avg_3_prior").desc_nulls_last(), "id")
w5 = Window.partitionBy("shipment_status", "channel").orderBy("event_seq", "id")
w6 = Window.partitionBy("store_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("shipping_fee_avg_3_prior", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("shipment_status")

with_latest = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.discount",
        "s.shipping_fee",
        F.col("c.delivered_date"),
        F.col("c.status").alias("shipment_status"),
    )
)

enriched = (
    with_latest
    .select(
        "id",
        "store_id",
        "channel",
        "discount",
        "shipping_fee",
        "shipment_status",
        F.row_number().over(w2).alias("event_seq"),
        F.avg("shipping_fee").over(w3).alias("shipping_fee_avg_3_prior"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    ranked
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id") == 35), "left_semi")
)

enriched_orders = (
    not_flagged
    .select(
        "id",
        "store_id",
        "discount",
        "shipping_fee",
        "shipping_fee_avg_3_prior",
        "dedup_rank",
        F.last("shipping_fee_avg_3_prior", ignorenulls=True).over(w6).alias("last_known_shipping_fee_avg_3_prior"),
        F.avg("shipping_fee").over(w_running).alias("shipping_fee_avg_2"),
        F.count("shipping_fee_avg_3_prior").over(w_recent).alias("shipment_status_count_shipping_fee_avg_3_prior"),
    )
)

summary = (
    enriched_orders
    .cube("store_id")
    .agg(
        F.percentile_approx("shipment_status_count_shipping_fee_avg_3_prior", [0.25, 0.5, 0.75]).alias("shipment_status_count_shipping_fee_avg_3_prior_quartiles"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.percentile_approx("shipping_fee_avg_2", [0.25, 0.5, 0.75]).alias("shipping_fee_avg_2_quartiles"),
    )
)

result = (
    summary
    .select(
        "store_id",
        "shipment_status_count_shipping_fee_avg_3_prior_quartiles",
        "distinct_store_ids",
        "shipping_fee_avg_2_quartiles",
    )
)

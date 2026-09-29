from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

not_flagged = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.quantity") == 6), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("shipment_status"))
)

matched_keys = (
    with_last
    .select("store_id", "id")
    .subtract(
        df_orders
        .filter(F.col("order_date") > "2024-09-01")
        .select("store_id", "id")
    )
)

verified = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter((F.col("order_date") > F.to_date(F.lit("2023-03-30"))) & (F.col("shipment_status").eqNullSafe(F.lit("LOST"))))
    .select(
        "*",
        F.date_sub("order_date", 1).alias("order_date_minus_1d"),
        F.coalesce("shipment_status", F.lit("N/A")).alias("shipment_status_filled"),
    )
)

remaining_keys = (
    base
    .select("channel", "customer_id")
    .subtract(
        df_orders
        .filter(F.col("customer_id").isNotNull())
        .select("channel", "customer_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    kept_rows
    .select("channel", "customer_id")
    .intersectAll(
        df_orders
        .filter(F.col("channel").isNotNull())
        .select("channel", "customer_id")
    )
)

verified_orders = (
    kept_rows
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

eligible = (
    verified_orders
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.quantity") <= 6), "left_anti")
)

totals = (
    eligible
    .withColumn("shipping_fee_tier", F.when(F.col("shipping_fee") >= 32, "PREMIUM").otherwise("STANDARD"))
    .cube("status", "shipping_fee_tier")
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("amount")).otherwise(0)).alias("mobile_amount"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
        F.sum(F.when(F.col("shipment_status") == "IN_TRANSIT", F.col("shipping_fee")).otherwise(0)).alias("in_transit_shipping_fee"),
        F.stddev_samp("discount").alias("discount_stddev"),
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.grouping("status").alias("grouping_id"),
    )
)

result = (
    totals
    .select(
        "status",
        "mobile_amount",
        "in_transit_shipping_fee",
        "distinct_customer_ids",
        "grouping_id",
        F.when(F.col("status").rlike("^(CANCELLED|PENDING)$"), "GROUP_B").when(F.col("status").rlike("^(COMPLETED|SHIPPED)$"), "EXTENDED").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("mobile_amount").asc_nulls_first())
)

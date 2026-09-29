from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

cleaned = (
    df_orders
    .filter(F.month("order_date").isin([1, 5, 7]) & (F.col("order_date") >= F.lit("2023-03-18").cast("date")))
    .select(
        "*",
        F.coalesce("channel", F.lit("unassigned")).alias("channel_filled"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.carrier").alias("shipment_carrier"))
)

remaining_keys = (
    with_last
    .select("id", "channel")
    .intersect(
        df_orders
        .filter(F.col("customer_id").isin([20, 23, 39]))
        .select("id", "channel")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_window = (
    reconciled
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

base = (
    with_window
    .filter(~F.col("shipment_carrier").eqNullSafe(F.lit("UPS")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        "channel_filled",
        "resolved_channel",
        "shipment_carrier",
        "promo_id",
        "promo_code",
        "discount_pct",
        (F.col("discount_pct") / F.nullif("discount", F.lit(0))).alias("discount_pct_per_discount"),
    )
)

base_orders = (
    base
    .filter((F.col("order_date") > F.lit("2023-09-21").cast("date")) & F.col("promo_code").isin(["WELCOME5", "VIP"]))
    .select(
        "*",
        F.coalesce("shipment_carrier", F.lit("N/A")).alias("shipment_carrier_filled"),
    )
)

result = (
    base_orders
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .groupingSets(
        [["region", "status"], ["region"], []],
        F.col("s.region").alias("region"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("a.promo_id")).alias("total_promo_id"),
        F.min(F.col("a.shipping_fee")).alias("min_shipping_fee"),
    )
)

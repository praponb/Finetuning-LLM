from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-04-05").cast("date"))
    .select("*", F.length("channel").alias("channel_length"))
)

with_window = (
    base
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

latest_match = (
    with_window
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("shipment_cost"), F.col("c.delivered_date"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.channel",
        "s.promo_id",
        "s.promo_code",
        "s.discount_pct",
        "s.shipment_cost",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

result = (
    with_latest
    .groupBy("promo_id")
    .agg(
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.percentile_approx("order_item_quantity", [0.25, 0.5, 0.75]).alias("order_item_quantity_quartiles"),
    )
    .filter(F.col("num_promo_codes") >= 1)
    .orderBy(F.col("promo_id").asc_nulls_first())
)

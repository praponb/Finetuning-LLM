from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region", "store_id").orderBy(F.col("shipping_fee").desc(), "id")
w3 = Window.orderBy("discount", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("order_item_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("partner"))) & (F.col("order_date") > F.lit("2023-03-25").cast("date")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.substring("status", 1, 2).alias("status_prefix"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.email").like("%@gmail.com"))
    .select("e.*", F.col("b.name"))
)

eligible = (
    enriched
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.to_date(F.col("ch.paid_at")) < "2023-12-05"), "left_anti")
)

joined = (
    eligible
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.region"))
)

enriched_orders = (
    joined
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.sqft"), F.col("b.store_name"))
)

with_last = (
    enriched_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("order_item_quantity"))
)

with_windows = (
    with_last
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "shipping_fee",
        "status_prefix",
        "channel_filled",
        "name",
        "region",
        "sqft",
        F.ntile(4).over(w2).alias("shipping_fee_bucket"),
        F.avg("sqft").over(w3).alias("sqft_avg_6"),
        F.sum("sqft").over(w4).alias("cumulative_sqft"),
    )
)

totals = (
    with_windows
    .withColumn("shipping_fee_tier", F.when(F.col("shipping_fee") >= 36, "HIGH").otherwise("LOW"))
    .rollup("shipping_fee_tier")
    .agg(
        F.avg("cumulative_sqft").alias("avg_cumulative_sqft"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    totals
    .select(
        "shipping_fee_tier",
        "avg_cumulative_sqft",
        "avg_shipping_fee",
        "num_regions",
    )
    .orderBy(F.col("avg_cumulative_sqft").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_orders
    .filter(~F.col("channel").eqNullSafe(F.lit("web")))
    .select(
        "*",
        F.round("amount", 2).alias("amount_rounded"),
        F.col("shipping_fee").cast("int").alias("shipping_fee_int"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

scored = (
    latest_match
    .filter(F.col("shipping_fee").isNull())
    .select(
        "*",
        F.min("discount").over(w_running).alias("last4_min_discount"),
        F.max("discount").over(w_running).alias("last4_max_discount"),
        F.count("discount").over(w_running).alias("last4_count_discount"),
        F.avg("discount").over(w_running).alias("last4_avg_discount"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.amount",
        "s.discount",
        "s.shipping_fee_int",
        "s.last4_min_discount",
        "s.last4_max_discount",
        "s.last4_count_discount",
        "s.last4_avg_discount",
        F.col("c.quantity"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "customer_id",
        "order_date",
        "discount",
        "last4_min_discount",
        "last4_count_discount",
        "last4_avg_discount",
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)

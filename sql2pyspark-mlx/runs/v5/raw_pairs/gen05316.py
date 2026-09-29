from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id", "store_id").orderBy("order_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("customer_id", "store_id").orderBy("prev_shipping_fee", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("prev_shipping_fee_int").desc(), "id")
w5 = Window.partitionBy("payment_method").orderBy(F.col("seq_no").desc(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_windows = (
    df_orders
    .filter(F.col("order_date") >= "2024-02-01")
    .select(
        "*",
        F.lag("discount").over(w_part).alias("prev_discount"),
        F.lead("discount").over(w_part).alias("next_discount"),
        F.lag("shipping_fee").over(w_part).alias("prev_shipping_fee"),
        F.lead("shipping_fee").over(w_part).alias("next_shipping_fee"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

valid = (
    with_windows
    .filter(F.col("order_date") > F.lit("2023-07-18").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "shipping_fee",
        "prev_discount",
        "next_discount",
        "prev_shipping_fee",
        "next_shipping_fee",
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

valid_orders = (
    valid
    .filter(F.col("store_id").isNotNull() & (F.col("prev_shipping_fee") < 25))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "shipping_fee",
        "prev_discount",
        "prev_shipping_fee",
        "next_shipping_fee",
        "status_title",
        F.round("shipping_fee", 0).alias("shipping_fee_rounded"),
        F.col("prev_shipping_fee").cast("int").alias("prev_shipping_fee_int"),
    )
)

latest_match = (
    valid_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.prev_discount",
        "s.prev_shipping_fee",
        "s.next_shipping_fee",
        "s.shipping_fee_rounded",
        "s.prev_shipping_fee_int",
        F.col("c.method").alias("payment_method"),
        F.col("c.amount"),
    )
)

enriched = (
    latest_match
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("seq_no_2", F.row_number().over(w4))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w6))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.quantity").alias("order_item_quantity"))
)

result = (
    with_last
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("s.region").alias("region"),
        F.col("s.city").alias("city"),
        F.col("e.payment_method").alias("payment_method"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.amount")).alias("max_amount"),
    )
)

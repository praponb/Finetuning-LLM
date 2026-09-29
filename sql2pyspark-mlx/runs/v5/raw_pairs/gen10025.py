from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc_nulls_last())
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id").orderBy(F.col("amount").desc(), "id")
w5 = Window.orderBy(F.col("shipping_fee").desc())
w_grp = Window.partitionBy("store_id").orderBy("shipping_fee_dense_rank", "id")
w7 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee_dense_rank_2").desc(), "id", "promo_id")
w9 = Window.partitionBy("promo_code").orderBy(F.col("shipping_fee_dense_rank_2_per_status_stddev_samp_discount").desc(), "id", "promo_id")
w10 = Window.partitionBy("promo_code").orderBy(F.col("shipping_fee").desc(), "id", "promo_id")

filtered = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-09-21").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.last_day("order_date").alias("order_date_month_end"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

with_windows = (
    filtered
    .filter(F.col("order_date").between("2023-07-01", "2024-12-01"))
    .withColumn("status_stddev_samp_discount", F.stddev_samp("discount").over(w))
    .withColumn("shipping_fee_dense_rank", F.dense_rank().over(w2))
    .withColumn("corr_shipping_fee_discount", F.corr("shipping_fee", "discount").over(w3))
    .withColumn("amount_bucket", F.ntile(5).over(w4))
    .withColumn("shipping_fee_dense_rank_2", F.dense_rank().over(w5))
)

metrics = (
    with_windows
    .withColumn("next_amount", F.lead("amount").over(w_grp))
    .withColumn("next_amount_bucket", F.lead("amount_bucket").over(w_grp))
    .withColumn("prev_amount_bucket", F.lag("amount_bucket").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

filtered_orders = (
    metrics
    .filter(F.col("channel").isNotNull() & (F.col("row_seq") < 4))
    .withColumn("shipping_fee_dense_rank_2_per_status_stddev_samp_discount", (F.col("shipping_fee_dense_rank_2") / F.nullif("status_stddev_samp_discount", F.lit(0))))
)

attributed = (
    filtered_orders
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_latest = (
    attributed
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.status",
        "s.shipping_fee",
        "s.order_date_month_end",
        "s.channel_filled",
        "s.status_stddev_samp_discount",
        "s.shipping_fee_dense_rank_2",
        "s.prev_amount_bucket",
        "s.row_seq",
        "s.shipping_fee_dense_rank_2_per_status_stddev_samp_discount",
        "s.promo_id",
        "s.promo_code",
        "s.discount_pct",
        F.col("c.status").alias("shipment_status"),
        F.col("c.carrier").alias("shipment_carrier"),
    )
)

result = (
    with_latest
    .withColumn("shipping_fee_dense_rank_2_bucket", F.ntile(3).over(w8))
    .withColumn("row_seq_2", F.row_number().over(w9))
    .withColumn("shipping_fee_bucket", F.ntile(5).over(w10))
)

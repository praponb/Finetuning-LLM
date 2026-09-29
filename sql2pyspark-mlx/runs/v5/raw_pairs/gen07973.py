from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("greatest_shipping_fee_amount").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("shipment_status").orderBy(F.col("greatest_shipping_fee_amount").desc())
w4 = Window.partitionBy("shipment_status").orderBy("greatest_shipping_fee_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("discount").desc())

base = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-09-24"))) & (F.col("amount") < 900))
    .withColumn("greatest_shipping_fee_amount", F.greatest("shipping_fee", "amount"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("N/A")))
)

kept = (
    base
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([8, 25, 26, 38])), "left_anti")
)

deduped = (
    kept
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.delivered_date") >= F.col("s.order_date")), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.discount",
        "s.shipping_fee",
        "s.greatest_shipping_fee_amount",
        "s.channel_filled",
        F.col("c.status").alias("shipment_status"),
    )
)

metrics = (
    latest_match
    .filter(F.col("greatest_shipping_fee_amount") <= 75)
    .select(
        "id",
        "discount",
        "greatest_shipping_fee_amount",
        "channel_filled",
        F.rank().over(w_part).alias("greatest_shipping_fee_amount_rank"),
        F.last("shipping_fee", ignorenulls=True).over(w4).alias("last_known_shipping_fee"),
        F.dense_rank().over(w_hist).alias("discount_dense_rank"),
    )
)

base_orders = (
    metrics
    .filter(F.col("discount_dense_rank") > 8)
    .select(
        "id",
        "discount",
        "channel_filled",
        "greatest_shipping_fee_amount_rank",
        "last_known_shipping_fee",
        "discount_dense_rank",
        F.when(F.col("greatest_shipping_fee_amount_rank") >= 8, "High").when(F.col("greatest_shipping_fee_amount_rank") >= 4, "Medium").otherwise("Low").alias("greatest_shipping_fee_amount_rank_band"),
        (F.col("greatest_shipping_fee_amount") + F.col("discount")).alias("greatest_shipping_fee_amount_plus_discount"),
    )
)

result = (
    base_orders
    .groupBy(
        F.when(F.col("greatest_shipping_fee_amount_plus_discount") >= 68, "HIGH").otherwise("LOW").alias("greatest_shipping_fee_amount_plus_discount_tier"),
    )
    .agg(
        F.percentile_approx("discount", 0.9).alias("discount_p90"),
        F.percentile_approx("discount", 0.5).alias("discount_p50"),
    )
)

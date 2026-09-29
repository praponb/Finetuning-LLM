from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy(F.col("discount").asc_nulls_last())
w3 = Window.partitionBy("payment_status", "customer_id").orderBy("discount", "id")
w4 = Window.orderBy(F.col("discount_times_shipping_fee").desc())

valid = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-09-04").cast("date")) & (F.col("shipping_fee") < 26))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        (F.col("discount") * F.col("shipping_fee")).alias("discount_times_shipping_fee"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("payment_status"))
)

kept = (
    with_last
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id") == 16), "left_anti")
)

enriched = (
    kept
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "payment_status",
        F.rank().over(w2).alias("discount_rank"),
        F.lag("discount_times_shipping_fee", 1).over(w3).alias("prev_discount_times_shipping_fee"),
        F.rank().over(w4).alias("discount_times_shipping_fee_rank"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("prev_discount_times_shipping_fee") >= 75, "LARGE").otherwise("SMALL").alias("prev_discount_times_shipping_fee_tier"),
    )
    .agg(
        F.round(F.sum("discount_rank") / F.nullif(F.count(F.when(F.col("status") == "SHIPPED", 1)), F.lit(0)), 2).alias("discount_rank_per_shipped"),
        F.count_if(F.col("payment_status") == "REFUNDED").alias("refunded_events"),
    )
    .filter(F.col("discount_rank_per_shipped") > 0)
    .orderBy(F.col("refunded_events").desc())
)

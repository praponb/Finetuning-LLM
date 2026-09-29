from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w4 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("shipping_fee", "id")
w6 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("cumulative_amount_with_shipping").asc_nulls_first())

valid = (
    df_orders
    .filter(F.col("order_date") >= "2024-12-01")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        (F.col("amount") * F.col("shipping_fee")).alias("amount_with_shipping"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

metrics = (
    valid
    .filter(F.col("amount_with_shipping") < 75)
    .select(
        "*",
        F.sum("amount_with_shipping").over(w).alias("cumulative_amount_with_shipping"),
        F.sum("shipping_fee").over(w_ordered).alias("cumulative_shipping_fee"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        "s.amount_with_shipping",
        "s.channel_clean",
        "s.cumulative_amount_with_shipping",
        F.col("c.unit_price"),
    )
)

enriched = (
    latest_match
    .select(
        "*",
        F.sum("discount").over(w4).alias("cumulative_discount"),
        F.lead("shipping_fee", 1).over(w5).alias("next_shipping_fee"),
        F.first("unit_price", ignorenulls=True).over(w6).alias("first_known_unit_price"),
        F.dense_rank().over(w7).alias("cumulative_amount_with_shipping_dense_rank"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") >= "2024-10-01"), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("shipping_fee").between(27, 31))
    .withColumn("cumulative_amount_with_shipping_dense_rank_num", F.col("cumulative_amount_with_shipping_dense_rank").cast("double"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("cumulative_amount_with_shipping_dense_rank") >= 6, "LARGE").otherwise("SMALL").alias("cumulative_amount_with_shipping_dense_rank_tier"),
    )
    .agg(
        F.round(F.avg("first_known_unit_price"), 2).alias("avg_first_known_unit_price"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("avg_shipping_fee") >= 0)
)

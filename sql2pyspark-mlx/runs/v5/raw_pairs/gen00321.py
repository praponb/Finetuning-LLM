from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("customer_id").orderBy("discount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("next_shipping_fee", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("store_id").orderBy(F.col("prev_discount").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_dims = (
    df_orders
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.store_id",
        "e.status",
        "e.discount",
        "e.shipping_fee",
        F.col("d.city"),
    )
)

eligible = (
    with_dims
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.product_id") == 17), "left_anti")
)

enriched = (
    eligible
    .select(
        "id",
        "store_id",
        "discount",
        F.lead("discount").over(w).alias("next_discount"),
        F.lag("discount").over(w).alias("prev_discount"),
        F.lead("shipping_fee").over(w).alias("next_shipping_fee"),
        F.row_number().over(w).alias("event_seq"),
        F.avg("discount").over(w2).alias("discount_avg_3"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.avg("next_discount").over(w3).alias("next_discount_avg_6_prior"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([9, 10, 11, 14])), "left_semi")
)

kept_orders = (
    kept
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isNotNull()), "left_semi")
)

with_latest = (
    kept_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("payment_amount"))
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("payment_amount") >= 1640, "LARGE").otherwise("SMALL").alias("payment_amount_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("amount", "id")
w2 = Window.partitionBy("store_id", "customer_id").orderBy("shipping_fee", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("order_date") >= F.lit("2023-07-08").cast("date")))
    .withColumn("shipping_fee_rounded", F.round("shipping_fee", 2))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("NONE")))
)

enriched = (
    base
    .filter(F.col("discount") >= 0.37)
    .select(
        "id",
        "order_date",
        "amount",
        "discount",
        "shipping_fee",
        "shipping_fee_rounded",
        F.lead("discount").over(w).alias("next_discount"),
        F.lag("discount").over(w).alias("prev_discount"),
        F.lag("shipping_fee").over(w).alias("prev_shipping_fee"),
        F.lead("shipping_fee").over(w).alias("next_shipping_fee"),
        F.row_number().over(w).alias("event_seq"),
        F.avg("discount").over(w2).alias("discount_avg_3"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isNotNull()), "left_anti")
)

base_orders = (
    not_flagged
    .filter(F.col("discount") < 0.38)
    .withColumn("next_discount_filled", F.coalesce("next_discount", F.lit(0)))
    .withColumn("shipping_fee_filled", F.coalesce("shipping_fee", F.lit(0)))
)

base_orders_2 = (
    base_orders
    .filter((F.col("order_date") >= F.lit("2023-09-05").cast("date")) & (F.col("shipping_fee_rounded") > 36))
    .select(
        "*",
        F.least("next_discount_filled", "discount_avg_3").alias("least_next_discount_filled_discount_avg_3"),
    )
)

daily = (
    base_orders_2
    .groupBy(
        F.when(F.col("discount") >= 0.34, "HIGH").otherwise("LOW").alias("discount_tier"),
        F.when(F.col("prev_discount") >= 0.42, "HIGH").otherwise("LOW").alias("prev_discount_tier"),
        F.date_trunc("quarter", "order_date").alias("quarter_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("next_shipping_fee").alias("total_next_shipping_fee"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)

result = (
    daily
    .select(
        "discount_tier",
        "prev_discount_tier",
        "quarter_start",
        "distinct_ids",
        "total_next_shipping_fee",
        "distinct_ids_2",
    )
)

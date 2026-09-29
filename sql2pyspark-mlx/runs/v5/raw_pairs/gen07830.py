from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("order_date", "id")
w2 = Window.partitionBy("store_id", "customer_id").orderBy(F.col("discount").desc(), "id")
w3 = Window.orderBy(F.col("shipping_fee").asc_nulls_last())
w4 = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")
w_ordered = Window.partitionBy("store_id").orderBy("shipping_fee_dense_rank", "id")
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").asc_nulls_first())

filtered = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("partner"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.when(F.col("channel").isin(["web", "store"]), "Core").otherwise("Other").alias("channel_group"),
        F.when(F.col("discount") >= 0.37, "High").when(F.col("discount") >= 0.1, "Medium").otherwise("Low").alias("discount_tier"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "shipping_fee",
        "channel_group",
        "discount_tier",
        F.lag("shipping_fee", 1).over(w).alias("prev_shipping_fee"),
        F.row_number().over(w2).alias("seq_no"),
        F.dense_rank().over(w3).alias("shipping_fee_dense_rank"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < F.col("s.order_date")), "left_anti")
)

latest = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .select(
        "id",
        "store_id",
        "order_date",
        "shipping_fee",
        "prev_shipping_fee",
        "seq_no",
        "shipping_fee_dense_rank",
        "dedup_rank",
        F.row_number().over(w_ordered).alias("event_seq"),
        F.max("shipping_fee_dense_rank").over(w6).alias("status_max_shipping_fee_dense_rank"),
        F.dense_rank().over(w7).alias("shipping_fee_dense_rank_2"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.delivered_date") >= F.col("s.order_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("status_max_shipping_fee_dense_rank") >= 9, "HIGH").otherwise("LOW").alias("status_max_shipping_fee_dense_rank_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
        F.max("status_max_shipping_fee_dense_rank").alias("max_status_max_shipping_fee_dense_rank"),
    )
)

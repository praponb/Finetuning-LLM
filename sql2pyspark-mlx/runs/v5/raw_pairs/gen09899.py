from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id")
w_part = Window.partitionBy("order_id").orderBy("paid_at", "id")
w3 = Window.partitionBy("order_id")
w_running = Window.partitionBy("method")
w5 = Window.partitionBy("order_id", "status").orderBy("amount_times_prev_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status", "order_id").orderBy("amount", "id")

scored = (
    df_payments
    .withColumn("amount_bucket", F.ntile(10).over(w))
    .withColumn("prev_amount", F.lag("amount", 1, 0).over(w_part))
)

base = (
    scored
    .filter((F.col("paid_at") >= F.lit("2023-09-22").cast("date")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "amount_bucket",
        "prev_amount",
        (F.col("amount") * F.col("prev_amount")).alias("amount_times_prev_amount"),
        F.coalesce("method", F.lit("DIRECT")).alias("resolved_method"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.stddev_samp("amount_bucket").over(w3).alias("order_id_stddev_samp_amount_bucket"),
        F.sum("amount").over(w_running).alias("method_sum_amount"),
    )
)

scored_payments = (
    enriched
    .withColumn("first_known_amount_bucket", F.first("amount_bucket", ignorenulls=True).over(w5))
    .withColumn("amount_bucket_2", F.ntile(10).over(w6))
)

enriched_payments = (
    scored_payments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.amount",
        "e.prev_amount",
        "e.resolved_method",
        "e.method_sum_amount",
        F.col("d.shipping_fee"),
        F.col("d.status").alias("order_status"),
    )
)

result = (
    enriched_payments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .cube(
        F.col("o.channel").alias("channel"),
        F.col("s.order_status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.shipping_fee")).alias("min_shipping_fee"),
    )
    .orderBy(F.col("channel").asc_nulls_first())
)

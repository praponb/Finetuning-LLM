from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy(F.col("shipping_fee").desc(), "id")
w3 = Window.partitionBy("payment_method").orderBy(F.col("payment_method_length").desc(), "id")
w4 = Window.orderBy("payment_method_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("shipping_fee", "id")

latest_match = (
    df_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.status",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        F.col("c.status").alias("payment_status"),
        F.col("c.method").alias("payment_method"),
    )
)

deduped = (
    latest_match
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_date", "shipping_fee", "payment_method")
)

filtered = (
    deduped
    .filter(F.col("order_date") >= F.lit("2023-02-27").cast("date"))
    .withColumn("payment_method_group", F.when(F.col("payment_method").isin(["paypal", "card"]), "Priority").otherwise("Other"))
    .withColumn("payment_method_length", F.length("payment_method"))
)

with_windows = (
    filtered
    .select(
        "id",
        "payment_method_length",
        F.row_number().over(w3).alias("event_seq"),
        F.sum("payment_method_length").over(w4).alias("cumulative_payment_method_length"),
        F.row_number().over(w5).alias("event_seq_2"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id") == 27), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.status") == "IN_TRANSIT"), "left_anti")
)

kept_orders = (
    kept
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.quantity").between(4, 6)), "left_semi")
)

result = (
    kept_orders
    .groupBy(
        F.when(F.col("event_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
        F.when(F.col("event_seq_2") >= 7, "LARGE").otherwise("SMALL").alias("event_seq_2_tier"),
        F.when(F.col("cumulative_payment_method_length") >= 335, "LARGE").otherwise("SMALL").alias("cumulative_payment_method_length_tier"),
    )
    .agg(
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.percentile_approx("event_seq_2", [0.25, 0.5, 0.75]).alias("event_seq_2_quartiles"),
    )
)

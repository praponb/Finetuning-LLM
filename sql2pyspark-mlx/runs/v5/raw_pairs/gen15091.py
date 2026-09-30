from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

valid = (
    df_payments
    .filter(F.col("paid_at") >= F.to_timestamp(F.lit("2023-04-24 06:00:00")))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.trim("method").alias("clean_method"),
        F.date_format("paid_at", "yyyy-MM-dd").alias("paid_at_label"),
    )
)

with_prev = valid.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "paid_at_label",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

with_dims = (
    sessionized
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.month(F.col("p.order_date")).isin([4, 5, 6]))
    .select(
        "e.id",
        "e.order_id",
        "e.paid_at",
        "e.paid_at_label",
        "e.session_no",
        F.col("p.shipping_fee"),
    )
)

flagged = (
    with_dims
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_no_2", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "paid_at",
        "session_no",
        "is_new_burst",
        "session_no_2",
        F.col("min_shipment_cost"),
    )
)

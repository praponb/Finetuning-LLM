from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2024-04-18 05:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.ceil("amount").alias("amount_whole"),
        F.when(F.col("status") == "FAILED", 1).otherwise(0).alias("is_failed"),
        F.coalesce("method", F.lit("DIRECT")).alias("resolved_method"),
    )
)

enriched = (
    base
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.channel"), F.col("b.order_date"))
)

joined = (
    enriched
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.channel").alias("order_channel"))
)

prev_events = joined.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "amount_whole",
        "is_failed",
        "resolved_method",
        "order_channel",
        "prev_paid_at",
        F.col("max_shipment_cost"),
    )
    .orderBy(F.col("prev_paid_at").desc())
)

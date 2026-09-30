from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status")
w2 = Window.partitionBy("status", "method")
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-06-29 23:00:00").cast("timestamp"))
    .withColumn("status_group", F.when(F.col("status").isin(["REFUNDED", "SUCCESS"]), "Group A").otherwise("Other"))
)

metrics = (
    base
    .withColumn("order_id_max_amount", F.max("amount").over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
)

prev_events = metrics.withColumn("prev_paid_at", F.lag("paid_at").over(w3))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w4))

cleaned = (
    sessions_payments
    .filter(F.col("method").isNotNull() & F.col("amount").between(1100, 1360))
    .withColumn("prev_paid_at_weekofyear", F.weekofyear("prev_paid_at"))
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.amount").alias("order_amount"), F.col("d.discount"))
)

result = (
    with_dims
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "status_group",
        "order_id_max_amount",
        "prev_paid_at",
        "is_new_burst",
        "burst_id",
    )
    .orderBy("is_new_burst")
)

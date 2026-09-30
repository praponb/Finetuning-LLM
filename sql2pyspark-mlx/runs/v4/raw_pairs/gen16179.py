from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessions_payments
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "prev_paid_at",
        "is_new_session",
        "burst_id",
        F.when(F.col("status").rlike("^(REFUNDED|FAILED)$"), "GROUP_B").when(F.col("status").rlike("^(SUCCESS)$"), "CORE").otherwise("PRIORITY").alias("status_zone"),
        F.col("sum_order_item_quantity"),
    )
    .orderBy(F.col("is_new_session").desc(), "status")
)

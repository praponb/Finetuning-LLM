from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

flagged = (
    df_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    islands
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "is_new_session",
        F.when(F.col("status").rlike("^(REFUNDED|FAILED)$"), "GROUP_A").otherwise("GROUP_B").alias("status_zone"),
        F.col("avg_shipment_cost"),
    )
    .orderBy(F.col("method").asc_nulls_last())
)

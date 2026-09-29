from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

flagged = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .join(cost_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "method", "paid_at", F.col("max_shipment_cost"))
    .orderBy(F.col("method").desc())
)

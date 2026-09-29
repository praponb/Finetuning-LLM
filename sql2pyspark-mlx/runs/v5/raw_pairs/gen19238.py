from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    flagged
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_payments
    .join(amount_by_order_id, on="order_id", how="left")
    .select("id", "order_id", F.col("sum_payment_amount"))
    .orderBy(F.col("id").desc_nulls_first())
)

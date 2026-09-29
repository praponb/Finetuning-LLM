from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

with_gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "session_start_flag",
        "session_no",
        F.col("max_payment_amount"),
    )
    .orderBy("amount")
)

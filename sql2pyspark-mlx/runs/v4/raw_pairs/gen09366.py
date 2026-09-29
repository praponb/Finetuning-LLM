from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.min("amount").alias("min_payment_amount"))

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "session_id",
        F.when(F.col("method").rlike("^(promptpay|cash)$"), "EXTENDED").otherwise("GROUP_B").alias("method_zone"),
        F.col("min_payment_amount"),
    )
)

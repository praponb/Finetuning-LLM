from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.avg("amount").alias("avg_payment_amount"))

gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "is_new_burst",
        F.when(F.col("method").rlike("^(bank_transfer|card)$"), "GROUP_A").when(F.col("method").rlike("^(paypal|cash)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("method_zone"),
        F.col("avg_payment_amount"),
    )
)

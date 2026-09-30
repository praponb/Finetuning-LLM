from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "new_group_flag",
        "burst_id",
        F.when(F.col("method").rlike("^(cash|bank_transfer)$"), "CORE").when(F.col("method").rlike("^(card|paypal)$"), "GROUP_B").otherwise("GROUP_A").alias("method_zone"),
        F.col("max_payment_amount"),
    )
)

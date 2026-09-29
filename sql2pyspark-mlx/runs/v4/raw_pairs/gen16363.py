from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_payments
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "is_new_session",
        F.when(F.col("method").rlike("^(cash|promptpay)$"), "GROUP_B").when(F.col("method").rlike("^(card|paypal)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("method_zone"),
    )
    .orderBy(F.col("method_zone").asc_nulls_last(), "id")
)

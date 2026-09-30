from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessions_payments
    .select(
        "id",
        "method",
        "paid_at",
        "is_new_session",
        F.when(F.col("method").rlike("^(bank_transfer|cash)$"), "CORE").when(F.col("method").rlike("^(card|paypal)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("method_zone"),
    )
    .orderBy(F.col("is_new_session").desc_nulls_last())
)

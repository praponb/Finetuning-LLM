from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessions_payments
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        "prev_paid_at",
        "is_new_session",
        "burst_id",
        F.when(F.col("method").rlike("^(card|promptpay)$"), "CORE").otherwise("EXTENDED").alias("method_zone"),
    )
)

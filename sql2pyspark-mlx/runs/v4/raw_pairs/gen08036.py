from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessions_payments
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "is_new_burst",
        F.when(F.col("status").rlike("^(REFUNDED|FAILED)$"), "CORE").otherwise("GROUP_A").alias("status_zone"),
    )
)

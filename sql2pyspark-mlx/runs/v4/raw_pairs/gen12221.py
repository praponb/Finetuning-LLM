from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "prev_paid_at",
        "is_new_session",
        "session_id",
        F.when(F.col("method").rlike("^(cash|card)$"), "GROUP_A").otherwise("PRIORITY").alias("method_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("amount") > 1460)
    .withColumn("is_refunded", F.when(F.col("status") == "REFUNDED", 1).otherwise(0))
)

prev_events = valid.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .withColumn("session_no_tier", F.when(F.col("session_no") >= 7, "PREMIUM").otherwise("STANDARD"))
    .withColumn("is_refunded_tier", F.when(F.col("is_refunded") >= 66, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["session_no_tier", "is_refunded_tier"], ["session_no_tier"], []],
        "session_no_tier",
        "is_refunded_tier",
    )
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.grouping_id().alias("grouping_level"),
    )
)

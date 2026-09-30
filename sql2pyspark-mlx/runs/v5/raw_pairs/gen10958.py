from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("amount").between(660, 1300))
    .withColumn("amount_level", F.when(F.col("amount") >= 1700, "High").when(F.col("amount") >= 470, "Medium").otherwise("Low"))
)

prev_events = filtered.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    flagged
    .select(
        "id",
        "order_id",
        "paid_at",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("session_no") >= 8, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
    )
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.min("paid_at").alias("first_seen"),
        F.min("paid_at").alias("first_seen_2"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("discount").alias("min_order_discount"))

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_sessions
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "prev_started_at",
        "is_new_session",
        "session_id",
        F.when(F.col("device").rlike("^(mobile|desktop)$"), "OTHER_ZONE").otherwise("CORE").alias("device_zone"),
        F.col("min_order_discount"),
    )
)

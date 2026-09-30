from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .select("*", F.dayofweek("paid_at").alias("paid_at_dayofweek"))
)

prev_events = filtered.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy("method", "session_no", "order_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("method") == "promptpay", 1)), F.lit(0)), 2).alias("session_no_per_promptpay"),
    )
    .orderBy(F.col("session_no_per_promptpay").desc_nulls_first())
)

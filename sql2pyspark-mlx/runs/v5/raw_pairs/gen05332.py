from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

result = (
    sessionized
    .groupBy("method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes_2"),
        F.sort_array(F.collect_set("method")).alias("method_list_2"),
        F.max("paid_at").alias("last_seen"),
    )
    .orderBy(F.col("max_amount").asc_nulls_last())
)

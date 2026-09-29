from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("session_id") >= 7, "LARGE").otherwise("SMALL").alias("session_id_tier"),
    )
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.min("paid_at").alias("first_seen"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") >= 1)
    .orderBy(F.col("session_id_tier").desc_nulls_last())
)

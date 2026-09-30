from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method", "status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy("amount", "id")
w_grp = Window.partitionBy("method").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.partitionBy("method").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

metrics = (
    sessionized
    .select(
        "id",
        "method",
        "amount",
        "island_id",
        F.avg("island_id").over(w3).alias("island_id_avg_90d"),
        F.sum("amount").over(w4).alias("cumulative_amount"),
    )
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

metrics_payments = (
    deduped
    .select(
        "id",
        "method",
        F.sum("island_id").over(w_grp).alias("running_sum_island_id"),
        F.min("island_id").over(w_grp).alias("running_min_island_id"),
        F.max("island_id").over(w_grp).alias("running_max_island_id"),
        F.avg("island_id").over(w_grp).alias("running_avg_island_id"),
        F.sum("island_id").over(w_grp_2).alias("running_island_id"),
    )
)

result = (
    metrics_payments
    .select("id", "method", "running_sum_island_id", "running_avg_island_id")
    .orderBy("id")
)

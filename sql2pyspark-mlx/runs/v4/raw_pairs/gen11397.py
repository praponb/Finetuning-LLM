from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("paid_at").asc_nulls_first(), "id")
w5 = Window.partitionBy("status").orderBy("paid_at", "id")
w6 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_payments
    .select(
        "*",
        F.ntile(10).over(w_running).alias("amount_bucket"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
        F.first("method", ignorenulls=True).over(w3).alias("first_known_method"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

with_prev = deduped.withColumn("prev_paid_at", F.lag("paid_at").over(w5))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("session_start_flag").over(w6))

result = (
    sessionized
    .groupBy("method", "order_id")
    .pivot("status", ["SUCCESS", "FAILED"])
    .agg(F.sum("amount_bucket").alias("amount"))
    .select(
        "method",
        "order_id",
        F.col("SUCCESS").alias("success"),
        F.col("FAILED").alias("failed"),
    )
)

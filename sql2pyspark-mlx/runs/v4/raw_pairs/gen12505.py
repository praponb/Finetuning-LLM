from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "method").orderBy("paid_at", "id")
w4 = Window.partitionBy("order_id").orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2024-04-01 18:00:00"))) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.trim("status").alias("clean_status"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_clean"),
    )
)

flagged = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

metrics = (
    sessionized
    .select(
        "*",
        F.row_number().over(w3).alias("row_seq"),
        F.last("session_start_flag", ignorenulls=True).over(w4).alias("last_known_session_start_flag"),
        F.sum("session_id").over(w_running).alias("session_id_sum_7d"),
        F.sum("amount").over(w6).alias("cumulative_amount"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.discount"), F.col("b.order_date"))
)

result = (
    with_dims
    .groupBy("status")
    .pivot("method", ["card", "promptpay", "cash"])
    .agg(F.max("row_seq").alias("amount"))
)

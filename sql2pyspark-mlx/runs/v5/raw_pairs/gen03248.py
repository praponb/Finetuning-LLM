from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "method").orderBy("amount", "id")
w4 = Window.partitionBy("order_id", "status").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method", "order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

latest = (
    sessionized
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

with_dims = (
    latest
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.amount").alias("order_amount"),
        F.col("b.shipping_fee"),
        F.col("b.status"),
    )
)

enriched = (
    with_dims
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.order_date"))
)

scored = (
    enriched
    .filter(F.col("paid_at") < "2024-02-10")
    .select(
        "*",
        F.count("burst_id").over(w4).alias("running_count_burst_id"),
        F.avg("burst_id").over(w4).alias("running_avg_burst_id"),
        F.sum("burst_id").over(w4).alias("running_sum_burst_id"),
        F.max("burst_id").over(w4).alias("running_max_burst_id"),
        F.sum("order_amount").over(w5).alias("cumulative_order_amount"),
    )
)

result = (
    scored
    .groupBy("method", "order_id")
    .pivot("status", ["SHIPPED", "CANCELLED", "PENDING"])
    .agg(
        F.max("running_max_burst_id").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
)

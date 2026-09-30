from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w_running_2 = Window.partitionBy("method").orderBy("discount", "id")

filtered = (
    df_payments
    .filter(F.col("amount") > 1440)
    .select("*", F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"))
)

joined = (
    filtered
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.channel").isin(["web", "mobile", "partner"]))
    .select("e.*", F.col("d.discount"), F.col("d.shipping_fee"))
)

gaps = (
    joined
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

with_dims = (
    islands
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.paid_at",
        "e.discount",
        "e.burst_id",
        F.col("o.channel"),
    )
)

result = (
    with_dims
    .select(
        "*",
        F.sum("burst_id").over(w_running).alias("last4_sum_burst_id"),
        F.count("burst_id").over(w_running).alias("last4_count_burst_id"),
        F.min("burst_id").over(w_running).alias("last4_min_burst_id"),
        F.row_number().over(w_running_2).alias("seq_no"),
    )
)

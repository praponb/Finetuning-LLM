from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("session_no", "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_hist = Window.orderBy("session_no")
w7 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("status", "order_id").orderBy("prev_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

scored = (
    sessionized
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "session_start_flag",
        F.lag("amount", 2).over(w_running).alias("prev_amount"),
        F.first("amount", ignorenulls=True).over(w4).alias("first_known_amount"),
        F.sum("session_start_flag").over(w5).alias("session_start_flag_sum_7d"),
        F.cume_dist().over(w_hist).alias("session_no_cume_dist"),
        F.avg("amount").over(w7).alias("amount_avg_5_prior"),
    )
)

enriched = (
    scored
    .filter(F.col("session_start_flag_sum_7d").isNull())
    .withColumn("first_known_first_known_amount", F.first("first_known_amount", ignorenulls=True).over(w8))
    .withColumn("running_prev_amount", F.sum("prev_amount").over(w9))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.status").alias("order_status"),
        F.col("p.shipping_fee"),
        F.col("p.amount"),
    )
)

result = (
    with_dims
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .groupingSets(
        [["channel", "order_status"], ["channel"]],
        F.col("p.channel").alias("channel"),
        F.col("src.order_status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.session_no_cume_dist")).alias("max_session_no_cume_dist"),
        F.min(F.col("src.running_prev_amount")).alias("min_running_prev_amount"),
    )
    .orderBy(F.col("max_session_no_cume_dist").asc_nulls_first())
)

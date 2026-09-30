from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method", "status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc_nulls_last(), "id")
w6 = Window.partitionBy("channel").orderBy("paid_at", "id")
w7 = Window.partitionBy("channel").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("method")
w9 = Window.orderBy(F.col("is_new_burst").desc(), "id")
w10 = Window.orderBy("running_avg_is_new_burst", "id")
w11 = Window.partitionBy("method").orderBy("order_amount", "id")
w12 = Window.partitionBy("method").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

scored = (
    sessionized
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "is_new_burst",
        F.max("is_new_burst").over(w3).alias("running_max_is_new_burst"),
        F.avg("is_new_burst").over(w3).alias("running_avg_is_new_burst"),
        F.count("is_new_burst").over(w3).alias("running_count_is_new_burst"),
        F.last("is_new_burst", ignorenulls=True).over(w4).alias("last_known_is_new_burst"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.status") == "RETURNED")
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.amount",
        "a.paid_at",
        "a.is_new_burst",
        "a.running_avg_is_new_burst",
        F.col("d.channel"),
        F.col("d.amount").alias("order_amount"),
    )
)

deduped = (
    joined
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_gaps_2 = (
    deduped
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w6).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w6) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps_2.withColumn("session_no", F.sum("new_group_flag").over(w7))

result = (
    islands
    .select(
        "id",
        "paid_at",
        "is_new_burst",
        "new_group_flag",
        F.round((F.col("order_amount") / F.sum("order_amount").over(w8)), 4).alias("order_amount_share"),
        F.row_number().over(w9).alias("seq_no"),
        F.ntile(3).over(w10).alias("running_avg_is_new_burst_bucket"),
        F.ntile(5).over(w11).alias("order_amount_bucket"),
        F.sum("is_new_burst").over(w12).alias("running_is_new_burst"),
    )
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("channel").orderBy("order_amount", "id")

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-09-14").cast("date")) & (F.col("order_id") == 26))
    .withColumn("paid_at_date", F.to_date("paid_at"))
    .withColumn("method_filled", F.coalesce("method", F.lit("N/A")))
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

enriched = (
    islands
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.amount",
        "a.paid_at",
        "a.status",
        "a.method_filled",
        "a.is_new_burst",
        "a.session_no",
        F.col("p.status").alias("order_status"),
        F.col("p.discount"),
    )
)

with_gaps = (
    enriched
    .withColumn("is_new_burst_2", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("island_id", F.sum("is_new_burst_2").over(w4))

joined = (
    sessions_payments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.discount").isNotNull())
    .select(
        "src.*",
        F.col("o.amount").alias("order_amount"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "method_filled",
        "session_no",
        "order_status",
        "is_new_burst_2",
        F.lead("discount").over(w_grp).alias("next_discount"),
        F.lag("discount").over(w_grp).alias("prev_discount"),
        F.lead("session_no").over(w_grp).alias("next_session_no"),
        F.lag("session_no").over(w_grp).alias("prev_session_no"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

by_type = (
    with_windows
    .groupBy("status")
    .pivot("order_status", ["COMPLETED", "PENDING"])
    .agg(F.max("session_no").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "status",
        F.col("COMPLETED_value_sum").alias("completed_value_sum"),
        F.col("COMPLETED_cnt").alias("completed_cnt"),
        F.col("PENDING_value_sum").alias("pending_value_sum"),
        F.col("PENDING_cnt").alias("pending_cnt"),
    )
)

result = (
    by_type
    .select(
        "status",
        "completed_value_sum",
        "completed_cnt",
        F.when(F.col("status").rlike("^(REFUNDED|FAILED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(SUCCESS)$"), "CORE").otherwise("PRIORITY").alias("status_zone"),
    )
)

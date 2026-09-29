from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("order_id", "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method", "order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("paid_at", "id")
w5 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("method").orderBy("paid_at", "id")
w7 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w9 = Window.partitionBy("method").orderBy(F.col("burst_id").desc(), "id")

metrics = (
    df_payments
    .filter(F.hour("paid_at").between(9, 17))
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.max("amount").over(w).alias("amount_max_7d"),
        F.last("method", ignorenulls=True).over(w2).alias("last_known_method"),
        F.last("method", ignorenulls=True).over(w3).alias("last_known_method_2"),
    )
)

flagged = (
    metrics
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w4).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w4) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_no", F.sum("new_group_flag").over(w5))

gaps = (
    sessions_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w6).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w6) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "method",
        "paid_at",
        "last_known_method",
        "last_known_method_2",
        "new_group_flag",
        F.sum("is_new_session").over(w7).alias("burst_id"),
    )
)

enriched = (
    sessionized
    .select(
        "id",
        "method",
        "last_known_method_2",
        "new_group_flag",
        "burst_id",
        F.lag("burst_id", 2).over(w6).alias("prev_burst_id"),
        F.sum("new_group_flag").over(w_part).alias("new_group_flag_sum_30d"),
        F.ntile(4).over(w9).alias("burst_id_bucket"),
    )
)

daily = (
    enriched
    .groupBy(
        F.when(F.col("new_group_flag") >= 1, "HIGH").otherwise("LOW").alias("new_group_flag_tier"),
        F.when(F.col("prev_burst_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("prev_burst_id_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
    )
)

result = (
    daily
    .select("new_group_flag_tier", "prev_burst_id_tier", "event_count", "row_count")
    .orderBy(F.col("prev_burst_id_tier").desc_nulls_last(), "new_group_flag_tier")
    .limit(10)
)

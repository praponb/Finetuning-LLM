from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("session_no_2", "id")
w6 = Window.partitionBy("device").orderBy("session_start_flag_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("device").orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("browser", "device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w9 = Window.partitionBy("customer_id").orderBy("session_no_2", "id")

with_gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

gaps = (
    sessionized
    .withColumn("session_start_flag_2", F.when(F.col("started_at") - F.lag("started_at").over(w3) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_no_2", F.sum("session_start_flag_2").over(w4))

metrics = (
    sessions_sessions
    .withColumn("prev_session_no_2", F.lag("session_no_2", 1).over(w5))
    .withColumn("running_session_no_2", F.sum("session_no_2").over(w6))
    .withColumn("running_session_start_flag", F.sum("session_start_flag").over(w_grp))
    .withColumn("session_start_flag_2_avg_7d", F.avg("session_start_flag_2").over(w_recent))
    .withColumn("session_no_2_bucket", F.ntile(4).over(w9))
)

gaps_2 = (
    metrics
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = gaps_2.withColumn("session_id", F.sum("is_new_session").over(w4))

result = (
    islands
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "session_start_flag",
        "session_no",
        "prev_session_no_2",
        "running_session_no_2",
        F.when(F.col("device").rlike("^(mobile|desktop)$"), "PRIORITY").otherwise("EXTENDED").alias("device_zone"),
    )
    .orderBy("country")
)

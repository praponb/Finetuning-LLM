from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.date_trunc("quarter", "prev_started_at").alias("quarter_start"),
        "browser",
    )
    .agg(
        F.min("prev_started_at").alias("first_seen"),
        F.max("started_at").alias("last_started_at"),
    )
    .orderBy(F.col("quarter_start").desc())
)

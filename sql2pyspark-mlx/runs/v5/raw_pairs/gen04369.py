from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

kept = (
    islands
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

kept_sessions = (
    kept
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.prev_started_at")), "left_anti")
)

kept_sessions_2 = (
    kept_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.prev_started_at")), "left_anti")
)

result = (
    kept_sessions_2
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .cube(F.col("c.segment").alias("segment"), F.col("a.device").alias("device"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.session_start_flag")), 2).alias("total_session_start_flag"),
        F.round(F.sum(F.col("a.session_id")), 2).alias("total_session_id"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("segment").desc_nulls_first())
)

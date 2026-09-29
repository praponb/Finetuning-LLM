from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id")
w4 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

gaps_2 = (
    islands
    .withColumn("new_group_flag_2", F.when(F.lag("started_at").over(w3).isNull() | (F.col("started_at") > F.lag("started_at").over(w3) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps_2.withColumn("session_no", F.sum("new_group_flag_2").over(w4))

not_flagged = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_semi")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id").isin([10, 36, 37])), "left_anti")
)

result = (
    kept
    .groupBy("browser", "session_no")
    .agg(
        F.sum("burst_id").alias("total_burst_id"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_rows"),
        F.min("started_at").alias("started_at"),
        F.count("*").alias("row_count"),
    )
    .orderBy("total_burst_id")
)

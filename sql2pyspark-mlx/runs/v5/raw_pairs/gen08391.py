from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

flagged = (
    islands
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

with_prev = sessionized.withColumn("prev_started_at", F.lag("started_at").over(w3))

with_gaps_2 = (
    with_prev
    .withColumn("new_group_flag_2", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands_sessions = (
    with_gaps_2
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "new_group_flag",
        "session_id",
        "session_id_2",
        F.sum("new_group_flag_2").over(w4).alias("session_id_3"),
    )
)

flagged_2 = (
    islands_sessions
    .withColumn("new_group_flag_2", F.when(F.lag("started_at").over(w3).isNull() | (F.col("started_at") > F.lag("started_at").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged_2.withColumn("session_id_4", F.sum("new_group_flag_2").over(w4))

result = (
    sessions_sessions
    .withColumn("year_start", F.date_trunc("year", "started_at"))
    .rollup("year_start", "session_id")
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.avg(F.when(F.col("device") == "desktop", F.col("new_group_flag_2"))).alias("desktop_avg_new_group_flag_2"),
        F.sum(F.when(F.col("device") == "tablet", F.col("new_group_flag"))).alias("tablet_total_new_group_flag"),
        F.count("*").alias("event_count"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("device") == "tablet", 1)), F.lit(0)), 2).alias("session_id_per_tablet"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("session_id_per_tablet").desc_nulls_last())
)

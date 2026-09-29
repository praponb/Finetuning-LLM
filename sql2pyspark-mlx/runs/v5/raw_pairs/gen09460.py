from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

result = (
    sessionized
    .withColumn("session_no_tier", F.when(F.col("session_no") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .groupingSets(
        [["session_no_tier", "year_start"], ["session_no_tier"]],
        "session_no_tier",
        "year_start",
    )
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.percentile_approx("session_no", 0.5).alias("session_no_p50"),
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.grouping_id().alias("gid"),
    )
)

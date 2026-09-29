from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    flagged
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessions_sessions
    .withColumn("session_no_tier", F.when(F.col("session_no") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .rollup("session_no_tier", "year_start")
    .agg(
        F.count("*").alias("num_rows"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.avg(F.when(F.col("country") == "DE", F.col("session_no"))).alias("de_avg_session_no"),
        F.grouping("session_no_tier").alias("grouping_level"),
    )
    .orderBy(F.col("year_start").desc())
)

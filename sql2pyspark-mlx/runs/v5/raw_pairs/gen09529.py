from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_sessions = (
    flagged
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("burst_id") >= 7, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
        F.date_trunc("week", "started_at").alias("week_start"),
    )
    .agg(
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.min("started_at").alias("first_started_at"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
    .orderBy(F.col("week_start").desc())
)

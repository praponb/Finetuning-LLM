from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy("duration_sec_filled", "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("duration_sec_filled", "id")
w5 = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").desc(), "id")

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)

metrics = (
    valid
    .filter(F.col("page") == "/checkout")
    .select(
        "*",
        F.lead("duration_sec_filled", 2).over(w).alias("next_duration_sec_filled"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
    )
)

scored = (
    metrics
    .withColumn("last_known_next_duration_sec_filled", F.last("next_duration_sec_filled", ignorenulls=True).over(w3))
    .withColumn("prev_duration_sec_filled", F.lag("duration_sec_filled", 1).over(w_grp))
    .withColumn("duration_sec_bucket", F.ntile(4).over(w5))
)

remaining_keys = (
    scored
    .select("page", "event_type")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([9, 37]))
        .select("page", "event_type")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["country", "page"], ["country"], []],
        F.col("s.country").alias("country"),
        F.col("a.page").alias("page"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.duration_sec_bucket")).alias("avg_duration_sec_bucket"),
        F.sum(F.col("a.running_duration_sec")).alias("total_running_duration_sec"),
    )
    .orderBy(F.col("total_running_duration_sec").asc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec_rounded").asc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy("row_num", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("browser", "event_type").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-09-04 15:00:00").cast("timestamp")) & (F.col("page").eqNullSafe(F.lit("/product"))))
    .withColumn("duration_sec_rounded", F.round("duration_sec", 0))
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .filter(F.col("b.browser") == "Firefox")
    .select("e.*", F.col("b.browser"), F.col("b.country"))
)

scored = (
    enriched
    .select(
        "*",
        F.avg("duration_sec").over(w2).alias("last7_avg_duration_sec"),
        F.count("duration_sec").over(w2).alias("last7_count_duration_sec"),
        F.max("duration_sec").over(w2).alias("last7_max_duration_sec"),
    )
)

metrics = (
    scored
    .withColumn("last3_sum_last7_count_duration_sec", F.sum("last7_count_duration_sec").over(w3))
    .withColumn("last3_count_last7_count_duration_sec", F.count("last7_count_duration_sec").over(w3))
    .withColumn("last3_avg_last7_count_duration_sec", F.avg("last7_count_duration_sec").over(w3))
)

result = (
    metrics
    .select(
        "id",
        "event_time",
        "duration_sec",
        "duration_sec_rounded",
        "row_num",
        "browser",
        "last7_count_duration_sec",
        "last3_count_last7_count_duration_sec",
        "last3_avg_last7_count_duration_sec",
        F.when(F.col("browser").rlike("^(Safari|Chrome)$"), "PRIORITY").otherwise("GROUP_A").alias("browser_zone"),
    )
)

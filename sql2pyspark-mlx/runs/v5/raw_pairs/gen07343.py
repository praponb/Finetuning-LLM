from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "session_id").orderBy(F.col("duration_sec").asc_nulls_first(), "id")
w2 = Window.partitionBy("session_id").orderBy("rank_in_group")
w3 = Window.partitionBy("session_id", "country").orderBy(F.col("rank_in_group").desc())
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

with_dims = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select("s.id", "s.session_id", "s.duration_sec", F.col("d.country"))
)

ranked = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

deduped = (
    latest
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "duration_sec",
        "rank_in_group",
        "row_num",
        F.col("avg_web_event_duration_sec"),
    )
    .orderBy(F.col("rank_in_group").desc_nulls_last())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("duration_sec_scaled").desc_nulls_last(), "id")
w2 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc_nulls_first())

base = (
    df_web_events
    .filter(F.to_date("event_time") >= "2023-04-01")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.round((F.col("duration_sec") * 1.07), 2).alias("duration_sec_scaled"),
        F.coalesce("page", F.lit("N/A")).alias("resolved_page"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.browser") != "Chrome")
    .select(
        "e.id",
        "e.session_id",
        "e.page",
        "e.duration_sec",
        "e.duration_sec_scaled",
        "e.resolved_page",
        F.col("s.country"),
    )
)

ranked = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

ranked_web_events = (
    ranked
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "page", "duration_sec")
)

result = (
    ranked_web_events
    .select("id", "page", "duration_sec")
    .orderBy("id")
    .limit(10)
)

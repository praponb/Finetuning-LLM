from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id")
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("session_id")

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-08-23").cast("date")) & F.col("page").isNotNull() & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.coalesce("page", F.lit("N/A")).alias("resolved_page"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.lead("duration_sec", 2).over(w).alias("next_duration_sec"),
        F.ntile(4).over(w2).alias("duration_sec_bucket"),
        F.max("duration_sec").over(w3).alias("session_id_max_duration_sec"),
    )
)

enriched_web_events = (
    enriched
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.browser"))
)

result = (
    enriched_web_events
    .rollup("browser")
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.percentile_approx("duration_sec", 0.9).alias("duration_sec_p90"),
    )
    .orderBy(F.col("browser").asc_nulls_first())
)

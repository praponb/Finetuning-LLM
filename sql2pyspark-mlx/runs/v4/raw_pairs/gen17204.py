from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "event_type").orderBy(F.col("event_time").asc_nulls_last(), "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

enriched = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.country"))
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    ranked
    .filter((F.col("event_time") > F.lit("2023-05-24").cast("date")) & F.col("country").isNotNull() & ~F.col("event_type").isin(["search", "purchase"]))
    .select(
        "*",
        F.upper("event_type").alias("event_type_upper"),
        F.initcap(F.trim("page")).alias("page_title"),
    )
)

gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w3))

result = (
    sessionized
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "country",
        "dedup_rank",
        "event_type_upper",
        "page_title",
        "burst_id",
        F.when(F.col("country").rlike("^(US|TH)$"), "GROUP_B").otherwise("CORE").alias("country_zone"),
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("id").desc_nulls_last())
)

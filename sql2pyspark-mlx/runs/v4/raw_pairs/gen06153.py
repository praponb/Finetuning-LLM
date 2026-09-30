from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("duration_sec")

enriched = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.country"), F.col("s.device"))
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base = (
    ranked
    .filter(F.col("page").isNotNull() & F.col("event_type").isin(["search", "purchase"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "country",
        F.coalesce("device", F.lit("N/A")).alias("device_filled"),
        F.coalesce("country", F.lit("NONE")).alias("country_clean"),
    )
)

result = (
    base
    .select("id", "page", "duration_sec", "country")
    .orderBy(F.col("duration_sec").desc_nulls_first(), "country")
)

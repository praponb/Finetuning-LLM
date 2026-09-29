from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")

latest = (
    df_sessions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_semi")
)

valid = (
    eligible
    .filter(F.col("country").isNotNull() & (F.col("started_at") >= "2024-08-01"))
    .select(
        "id",
        "device",
        "country",
        "rank_in_group",
        F.length("country").alias("country_length"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_clean"),
    )
)

result = (
    valid
    .select("id", "device", "rank_in_group", "country_length", "country_clean")
    .orderBy(F.col("id").desc_nulls_first())
)

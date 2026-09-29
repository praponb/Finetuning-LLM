from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc_nulls_first(), "id")

base = (
    df_sessions
    .filter(F.col("started_at") >= F.to_date(F.lit("2023-04-05")))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.trim("country").alias("clean_country"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_anti")
)

eligible_sessions = (
    eligible
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.session_id") == 22), "left_anti")
)

deduped = (
    eligible_sessions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .groupBy("browser")
    .agg(
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("tablet_events").asc_nulls_first())
)

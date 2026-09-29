from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "device").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("rank_in_group").desc(), "id")
w_part = Window.partitionBy("device").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("age_tier").orderBy(F.col("num_countries").desc())

ranked = (
    df_sessions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.hour(F.col("x.event_time")).between(11, 14)), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    deduped
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w_part))
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.country",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

enriched = (
    latest_match
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.country",
        "a.web_event_duration_sec",
        "a.web_event_event_type",
        F.col("b.country").alias("customer_country"),
        F.col("b.age"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("age") >= 66, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
        "country",
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.round(F.avg("web_event_duration_sec"), 2).alias("avg_web_event_duration_sec"),
        F.round(F.avg("age"), 2).alias("avg_age"),
    )
    .withColumn("num_countries_percent_rank", F.percent_rank().over(w5))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-06-23").cast("date")) & F.col("device").isin(["tablet", "desktop", "mobile"]))
    .select("*", F.trim("device").alias("clean_device"))
)

with_dims = (
    base
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("src.*", F.col("d.email"), F.col("d.credit_limit"))
)

eligible = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.duration_sec").between(179, 470)), "left_semi")
)

with_dims_sessions = (
    eligible
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.city"), F.col("d.age"))
)

latest_match = (
    with_dims_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"), F.col("c.page").alias("web_event_page"))
)

summary = (
    latest_match
    .groupBy(
        F.date_trunc("WEEK", "started_at").alias("week_start"),
        F.date_trunc("YEAR", "started_at").alias("year_start"),
    )
    .agg(
        F.min("started_at").alias("started_at"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count_if(F.col("web_event_page") == "/home").alias("home_events"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
)

result = (
    summary
    .select("week_start", "year_start", "home_events", "jp_rows", "num_countries")
    .orderBy(F.col("home_events").desc_nulls_first())
)

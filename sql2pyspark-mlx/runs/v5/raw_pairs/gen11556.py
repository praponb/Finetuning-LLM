from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

with_last = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.started_at",
        "s.country",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.page").alias("web_event_page"),
    )
)

result = (
    with_last
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .groupingSets(
        [["year_start", "browser"], ["year_start"], []],
        "year_start",
        "browser",
    )
    .agg(
        F.min("web_event_event_time").alias("first_seen"),
        F.count_if(F.col("web_event_page") == "/cart").alias("cart_events"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_rows"),
        F.countDistinct("web_event_page").alias("distinct_web_event_pages"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.sort_array(F.collect_set("web_event_page")).alias("web_event_page_list"),
        F.grouping("year_start").alias("gid"),
    )
    .orderBy(F.col("year_start").asc_nulls_last())
)

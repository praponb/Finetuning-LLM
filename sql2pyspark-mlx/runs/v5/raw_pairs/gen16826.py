from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_country").orderBy("duration_sec", "id")
w3 = Window.partitionBy("browser").orderBy(F.col("duration_sec").desc(), "id")

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-06-18").cast("date")) & (F.col("device") == "mobile"))
    .select(
        "*",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
        F.coalesce("country", F.lit("OTHER")).alias("country_clean"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.browser",
        "src.started_at_label",
        "src.country_clean",
        F.col("p.name"),
        F.col("p.country").alias("customer_country"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_type").isin(["search", "page_view", "purchase"])), "left_anti")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.session_id").isin([20, 36])), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.started_at_label",
        "s.name",
        "s.customer_country",
        F.col("c.page"),
        F.col("c.duration_sec"),
    )
)

result = (
    with_last
    .withColumn("next_duration_sec", F.lead("duration_sec").over(w2))
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w2))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("duration_sec_bucket", F.ntile(3).over(w3))
)

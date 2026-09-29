from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter(~F.col("browser").eqNullSafe(F.lit("Chrome")))
    .select(
        "id",
        "customer_id",
        "browser",
        F.col("started_at").cast("date").alias("started_at_day"),
        F.length("device").alias("device_length"),
        F.coalesce("browser", F.lit("N/A")).alias("resolved_browser"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.email").like("%@startup.io"))
    .select(
        "e.id",
        "e.customer_id",
        "e.browser",
        "e.started_at_day",
        "e.device_length",
        "e.resolved_browser",
        F.col("b.country"),
        F.col("b.credit_limit"),
    )
)

latest_match = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device_length",
        "s.country",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id").isin([14, 27, 31, 35])), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "country",
        "web_event_duration_sec",
        F.when(F.col("country").rlike("^(JP|DE)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("country_zone"),
    )
)

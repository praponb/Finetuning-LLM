from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("country_filled", F.coalesce("country", F.lit("unassigned")))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("N/A")))
)

result = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.browser",
        "s.country_filled",
        "s.browser_filled",
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

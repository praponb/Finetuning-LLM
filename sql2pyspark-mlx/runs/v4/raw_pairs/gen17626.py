from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2024-01-18").cast("date")) & F.col("customer_id").isin([15, 20, 32, 37]))
    .select(
        "id",
        "device",
        "browser",
        "country",
        F.upper("device").alias("device_upper"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
        F.col("c.page").alias("web_event_page"),
    )
)

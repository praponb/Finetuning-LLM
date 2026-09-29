from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("event_time", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy("event_type_length", "id")

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.length("event_type").alias("event_type_length"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.browser"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_5"),
        F.first("duration_sec", ignorenulls=True).over(w3).alias("first_known_duration_sec"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "event_type_length",
        "browser",
        "last_known_browser",
        "duration_sec_avg_5",
        F.when(F.col("event_type").rlike("^(purchase|click)$"), "PRIORITY").otherwise("EXTENDED").alias("event_type_zone"),
    )
    .orderBy(F.col("event_type_length").desc())
)

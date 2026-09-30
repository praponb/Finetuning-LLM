from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("country") == "UK")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.substring("device", 1, 3).alias("device_prefix"),
        F.coalesce("browser", F.lit("DIRECT")).alias("browser_clean"),
    )
)

with_gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

eligible = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "browser_clean",
        "island_id",
    )
    .orderBy(F.col("country").desc())
)

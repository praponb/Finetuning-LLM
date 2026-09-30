from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-03-24"))) & F.col("customer_id").isin([27, 34, 35, 40]))
    .withColumn("device_group", F.when(F.col("device").isin(["desktop", "tablet"]), "Tier 1").otherwise("Other"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("DIRECT")))
)

result = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.page").alias("web_event_page"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

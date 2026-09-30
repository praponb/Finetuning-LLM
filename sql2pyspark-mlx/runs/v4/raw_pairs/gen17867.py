from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "country").orderBy(F.col("started_at").asc_nulls_last(), "id")

base = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.upper("device").alias("device_upper"),
        F.trim("device").alias("clean_device"),
        F.coalesce("country", F.lit("NONE")).alias("country_clean"),
    )
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid = (
    ranked
    .filter((F.col("started_at") > F.lit("2023-12-06 13:00:00").cast("timestamp")) & (F.col("country") == "UK"))
    .withColumn("browser_country", F.concat("browser", F.lit("/"), "country"))
    .withColumn("device_prefix", F.substring("device", 1, 3))
    .withColumn("resolved_country", F.coalesce("country", F.lit("N/A")))
)

eligible = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "device_upper",
        "browser_country",
        "device_prefix",
    )
)

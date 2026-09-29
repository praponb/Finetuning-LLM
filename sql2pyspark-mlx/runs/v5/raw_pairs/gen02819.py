from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-04-25").cast("date")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.substring("device", 1, 2).alias("device_prefix"),
        F.when(F.col("browser") == "Edge", 1).otherwise(0).alias("is_edge"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec") != 469), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"), F.col("c.page").alias("web_event_page"))
)

result = (
    with_latest
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.segment").alias("segment"),
        F.col("src.browser").alias("browser"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("src.duration_sec")).alias("avg_duration_sec"),
    )
    .orderBy(F.col("browser").asc_nulls_last())
)

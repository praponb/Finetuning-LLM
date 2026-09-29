from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-02-15 12:00:00"))) & F.col("browser").isNotNull())
    .withColumn("browser_length", F.length("browser"))
)

valid = (
    filtered
    .filter((F.col("started_at") > F.lit("2023-06-18 04:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.upper("device").alias("device_upper"),
        F.trim("device").alias("clean_device"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_clean"),
    )
)

filtered_sessions = (
    valid
    .filter(F.col("started_at") >= F.lit("2023-05-04").cast("date"))
    .select(
        "*",
        F.substring("device", 1, 2).alias("device_prefix"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

with_last = (
    filtered_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page").isin(["/home", "/product"])), "left_anti")
)

summary = (
    not_flagged
    .groupBy("country", "device")
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)

result = (
    summary
    .select("country", "device", "th_count", "num_devices", "num_countries")
    .orderBy(F.col("num_countries").desc_nulls_last())
)

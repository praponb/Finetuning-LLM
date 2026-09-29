from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "device").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter(F.col("device") != "tablet")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.lower("browser").alias("browser_lower"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_anti")
)

with_windows = (
    kept
    .select(
        "*",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type").alias("web_event_event_type"), F.col("c.page"))
)

valid = (
    with_latest
    .filter(F.col("started_at") >= F.to_date(F.lit("2023-10-10")))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "browser_lower",
        "last_known_country",
        "row_seq",
        "page",
        F.coalesce("row_seq", F.lit(0)).alias("row_seq_filled"),
        F.initcap(F.trim("device")).alias("device_title"),
    )
)

filtered = (
    valid
    .filter(F.col("started_at") > F.lit("2024-01-28").cast("date"))
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "browser_lower",
        "last_known_country",
        "row_seq",
        "page",
        "row_seq_filled",
        "device_title",
        F.substring("page", 1, 2).alias("page_prefix"),
        F.upper("browser").alias("browser_upper"),
    )
)

result = (
    filtered
    .groupBy("device")
    .pivot("page", ["/checkout", "/home", "/product"])
    .agg(F.max("row_seq"))
    .select(
        "device",
        F.col("/checkout").alias("checkout"),
        F.col("/home").alias("home"),
        F.col("/product").alias("product"),
    )
)

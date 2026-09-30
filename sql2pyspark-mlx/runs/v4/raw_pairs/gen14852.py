from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2023-03-21"))) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.substring("browser", 1, 3).alias("browser_prefix"),
        F.when(F.col("device").isin(["mobile", "tablet"]), "Priority").otherwise("Other").alias("device_group"),
        F.coalesce("country", F.lit("NONE")).alias("resolved_country"),
    )
)

metrics = (
    filtered
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w2))
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_last = (
    ranked
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"), F.col("c.event_time"))
)

joined = (
    with_last
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.country").alias("customer_country"))
)

filtered_sessions = (
    joined
    .filter(F.col("started_at") >= F.lit("2024-01-30 15:00:00").cast("timestamp"))
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "browser_prefix",
        "device_group",
        "latest_rank",
        "web_event_page",
        "event_time",
        "customer_country",
        F.col("event_time").cast("date").alias("event_time_day"),
        F.coalesce("customer_country", F.lit("UNKNOWN")).alias("resolved_customer_country"),
    )
)

result = (
    filtered_sessions
    .select(
        "id",
        "browser",
        "country",
        "browser_prefix",
        "device_group",
        "latest_rank",
        "web_event_page",
        "event_time",
        "customer_country",
    )
    .orderBy("country", F.col("browser").desc())
)

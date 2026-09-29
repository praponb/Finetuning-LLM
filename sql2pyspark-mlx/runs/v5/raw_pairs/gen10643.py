from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("customer_country").orderBy(F.col("web_event_event_time").desc(), "id")
w4 = Window.partitionBy("customer_country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device").orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("event_seq", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("customer_country").orderBy("event_seq", "id")
w9 = Window.partitionBy("device").orderBy("event_seq", "id").rowsBetween(-5, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "desktop"]))
    .select(
        "*",
        F.lower("country").alias("country_lower"),
        F.coalesce("browser", F.lit("DIRECT")).alias("resolved_browser"),
    )
)

with_dims = (
    filtered
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.device",
        "a.started_at",
        "a.country",
        "a.country_lower",
        F.col("d.signup_date"),
        F.col("d.age"),
        F.col("d.country").alias("customer_country"),
    )
)

with_last = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.signup_date",
        "s.customer_country",
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

metrics = (
    with_latest
    .select(
        "id",
        "device",
        "customer_country",
        F.row_number().over(w3).alias("event_seq"),
        F.first("customer_country", ignorenulls=True).over(w4).alias("first_known_customer_country"),
        F.last("customer_country", ignorenulls=True).over(w5).alias("last_known_customer_country"),
    )
)

filtered_sessions = (
    metrics
    .filter(F.col("event_seq").isNotNull() & (~F.col("customer_country").eqNullSafe(F.lit("SG"))))
    .select(
        "id",
        "device",
        "customer_country",
        "event_seq",
        "first_known_customer_country",
        F.concat_ws("-", "customer_country", "device").alias("customer_country_device_key"),
    )
)

scored = (
    filtered_sessions
    .select(
        "id",
        "device",
        "event_seq",
        "first_known_customer_country",
        "customer_country_device_key",
        F.first("event_seq", ignorenulls=True).over(w6).alias("first_known_event_seq"),
        F.avg("event_seq").over(w7).alias("event_seq_avg_5_prior"),
        F.avg("event_seq").over(w7).alias("event_seq_avg_5_prior_2"),
        F.ntile(4).over(w8).alias("event_seq_bucket"),
        F.avg("event_seq").over(w9).alias("event_seq_avg_5"),
    )
)

result = (
    scored
    .groupBy("device")
    .agg(
        F.count("*").alias("event_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.countDistinct("device").alias("distinct_devices"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("device")).alias("num_devices_2"),
    )
)

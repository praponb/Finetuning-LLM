from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("credit_limit").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.concat_ws(",", "device", "country").alias("device_country_key"),
        F.when(F.col("browser") == "Firefox", 1).otherwise(0).alias("is_firefox"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_filled"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .filter(F.col("c.signup_date") < "2023-07-10")
    .select(
        "e.id",
        "e.customer_id",
        "e.is_firefox",
        "e.browser_filled",
        F.col("c.credit_limit"),
        F.col("c.name"),
        F.col("c.city"),
    )
)

latest = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.event_type").alias("web_event_event_type"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w3))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"))
)

base = (
    with_latest
    .filter(F.col("customer_id").isNotNull())
    .withColumn("is_firefox_per_credit_limit", F.round((F.col("is_firefox") / F.col("credit_limit")), 2))
    .withColumn("resolved_web_event_event_type", F.coalesce("web_event_event_type", F.lit("N/A")))
)

result = (
    base
    .groupBy("city")
    .pivot("web_event_event_type", ["page_view", "search"])
    .agg(F.sum("credit_limit").alias("total"), F.count(F.lit(1)).alias("n"))
)

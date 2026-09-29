from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

valid = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.substring("country", 1, 2).alias("country_prefix"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.device",
        "a.country",
        "a.country_prefix",
        F.col("p.signup_date"),
    )
)

eligible = (
    joined
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= "2025-08-01"), "left_anti")
)

with_dims = (
    eligible
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.segment"))
)

not_flagged = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id").isin([10, 15])), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "id",
        "country",
        "country_prefix",
        "signup_date",
        "segment",
        F.row_number().over(w).alias("seq_no"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.country_prefix",
        "s.signup_date",
        F.col("c.page"),
        F.col("c.event_type"),
    )
)

result = (
    with_last
    .groupBy(F.date_trunc("WEEK", "signup_date").alias("week_start"))
    .agg(
        F.count_if(F.col("page") == "/search").alias("search_events"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.countDistinct("page").alias("distinct_pages"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("search_events") > 1)
)

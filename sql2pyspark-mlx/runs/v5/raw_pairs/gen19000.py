from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc())
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("page")

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-07-12"))) & F.col("browser").isNotNull())
    .withColumn("device_lower", F.lower("device"))
    .withColumn("started_at_label", F.date_format("started_at", "yyyy-MM-dd"))
    .withColumn("country_filled", F.coalesce("country", F.lit("UNKNOWN")))
)

enriched = (
    cleaned
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.credit_limit"), F.col("d.segment"))
)

eligible = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_type") == "page_view"), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.started_at",
        "s.credit_limit",
        "s.segment",
        F.col("c.page"),
    )
)

with_dims = (
    with_latest
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.country"),
        F.col("d.credit_limit").alias("customer_credit_limit"),
    )
)

kept_keys = (
    with_dims
    .select("country", "device")
    .intersectAll(
        df_sessions
        .filter(~F.col("country").isin(["US", "UK", "JP"]))
        .select("country", "device")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "customer_id",
        "started_at",
        "credit_limit",
        "page",
        "customer_credit_limit",
        F.cume_dist().over(w2).alias("credit_limit_cume_dist"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_30d"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4).alias("credit_limit_share"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w5)), 4).alias("credit_limit_share_2"),
    )
)

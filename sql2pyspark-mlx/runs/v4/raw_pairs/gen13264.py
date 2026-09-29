from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device").orderBy("started_at", "id")
w3 = Window.partitionBy("customer_id", "device").orderBy(F.col("event_seq").asc_nulls_last(), "id")
w4 = Window.partitionBy("customer_id").orderBy("rank_in_group", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w6 = Window.partitionBy("city", "customer_id").orderBy("rank_in_group", "id")

enriched = (
    df_sessions
    .select(
        "*",
        F.last("country", ignorenulls=True).over(w_recent).alias("last_known_country"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    latest
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.city"), F.col("d.signup_date"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.avg("event_seq").over(w4).alias("event_seq_avg_3"),
        F.row_number().over(w5).alias("row_seq"),
        F.row_number().over(w6).alias("row_seq_2"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.to_date(F.col("ch.event_time")) < "2023-11-26"), "left_semi")
)

with_dims_sessions = (
    eligible
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.country").alias("customer_country"),
        F.col("b.city").alias("customer_city"),
        F.col("b.signup_date").alias("customer_signup_date"),
    )
)

result = (
    with_dims_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "last_known_country",
        "rank_in_group",
        "event_seq_avg_3",
        "row_seq_2",
    )
    .orderBy(F.col("rank_in_group").desc_nulls_first())
)

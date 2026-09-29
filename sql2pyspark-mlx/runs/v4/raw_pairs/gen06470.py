from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("page").orderBy("started_at", "id")
w3 = Window.partitionBy("page", "device").orderBy("started_at", "id")
w4 = Window.partitionBy("page").orderBy("event_time", "id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("event_time").desc(), "id")

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"), F.col("c.event_time"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.event_time")), "left_anti")
)

joined = (
    kept
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.segment"), F.col("c.name"))
)

enriched = (
    joined
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("row_seq_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "page",
        "event_time",
        "segment",
        "name",
        "event_seq",
    )
    .orderBy(F.col("id").desc())
)

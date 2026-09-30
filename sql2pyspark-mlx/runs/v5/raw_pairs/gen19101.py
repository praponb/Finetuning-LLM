from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id")
w4 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-04-26").cast("date"))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.date_format("started_at", "MMM yyyy").alias("started_at_label"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"))
)

enriched = (
    with_latest
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.age"),
        F.col("p.country").alias("customer_country"),
        F.col("p.segment"),
    )
)

enriched_sessions = (
    enriched
    .select(
        "*",
        F.last("customer_country", ignorenulls=True).over(w2).alias("last_known_customer_country"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("age").over(w4).alias("running_age"),
    )
)

result = (
    enriched_sessions
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["customer_segment", "session_country"], ["customer_segment"], []],
        F.col("dim.segment").alias("customer_segment"),
        F.col("src.country").alias("session_country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("src.age")).alias("avg_age"),
        F.grouping_id().alias("level_id"),
    )
)

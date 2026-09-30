from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

with_gaps_2 = (
    islands
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps_2
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "island_id",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

matched_keys = (
    sessionized
    .select("customer_id")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id") == 17)
        .select("customer_id")
    )
)

kept_rows = (
    sessionized
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_anti")
)

result = (
    kept
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["segment", "browser"], ["segment"]],
        F.col("p.segment").alias("segment"),
        F.col("s.browser").alias("browser"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("s.session_no")).alias("avg_session_no"),
        F.grouping_id().alias("grouping_level"),
    )
)

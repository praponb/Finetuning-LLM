from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("burst_id")
w4 = Window.orderBy(F.col("event_time_month").asc_nulls_first())

cleaned = (
    df_web_events
    .filter((F.col("page").eqNullSafe(F.lit("/search"))) & F.col("session_id").isin([21, 31]))
    .select(
        "id",
        "session_id",
        "event_time",
        F.month("event_time").alias("event_time_month"),
    )
)

with_gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

base = (
    sessions_web_events
    .filter((F.col("event_time") >= F.lit("2023-03-27 09:00:00").cast("timestamp")) & (F.col("burst_id") != 7))
    .select(
        "id",
        "event_time",
        "event_time_month",
        "is_new_burst",
        "burst_id",
        F.date_trunc("MONTH", "event_time").alias("event_time_period"),
    )
)

kept_keys = (
    base
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("page") == "/home")
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .withColumn("burst_id_dense_rank", F.dense_rank().over(w3))
    .withColumn("event_time_month_percent_rank", F.percent_rank().over(w4))
)

result = (
    metrics
    .select(
        "id",
        "event_time",
        "event_time_month",
        "burst_id",
        "event_time_period",
        "burst_id_dense_rank",
        "event_time_month_percent_rank",
    )
    .orderBy("burst_id_dense_rank")
)

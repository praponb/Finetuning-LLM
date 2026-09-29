from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-05-03").cast("date")) & (~F.col("page").eqNullSafe(F.lit("/checkout"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.when(F.col("event_type").isin(["search", "click"]), "Tier 1").otherwise("Other").alias("event_type_group"),
    )
)

with_gaps = (
    base
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL").alias("is_new_session_tier"),
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("is_new_session", 0.5).alias("is_new_session_p50"),
        F.percentile_approx("is_new_session", [0.25, 0.5, 0.75]).alias("is_new_session_quartiles"),
        F.percentile_approx("burst_id", 0.9).alias("burst_id_p90"),
        F.min("event_time").alias("first_event_time"),
    )
)

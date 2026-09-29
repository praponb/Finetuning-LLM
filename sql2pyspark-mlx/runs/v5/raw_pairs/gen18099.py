from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("event_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("event_time", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time_period", "id")
w4 = Window.partitionBy("event_type").orderBy("event_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("event_type").orderBy(F.col("burst_id").desc(), "id")
w7 = Window.partitionBy("event_type")
w8 = Window.orderBy(F.col("burst_id").desc(), "id")

valid = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        F.date_trunc("WEEK", "event_time").alias("event_time_period"),
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.last("page", ignorenulls=True).over(w_grp).alias("last_known_page"),
        F.row_number().over(w2).alias("row_seq"),
        F.first("page", ignorenulls=True).over(w_grp).alias("first_known_page"),
    )
)

with_gaps = (
    metrics
    .withColumn("new_group_flag", F.when(F.lag("event_time_period").over(w3).isNull() | (F.col("event_time_period") > F.lag("event_time_period").over(w3) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "event_type",
        "event_time",
        "event_time_period",
        "event_type_page_key",
        F.sum("new_group_flag").over(w4).alias("burst_id"),
    )
)

metrics_web_events = (
    sessions_web_events
    .select(
        "id",
        "event_time",
        "event_type_page_key",
        "burst_id",
        F.sum("burst_id").over(w5).alias("running_burst_id"),
        F.row_number().over(w6).alias("event_seq"),
        F.avg("burst_id").over(w7).alias("event_type_avg_burst_id"),
        F.ntile(10).over(w8).alias("burst_id_bucket"),
    )
)

kept_keys = (
    metrics_web_events
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("session_id").isin([29, 39]))
        .select("id")
    )
)

verified = (
    metrics_web_events
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("burst_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.max("event_time").alias("last_seen"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.percentile_approx("burst_id", 0.9).alias("burst_id_p90"),
    )
    .filter(F.col("distinct_ids_2") < 1000)
    .orderBy("burst_id")
)

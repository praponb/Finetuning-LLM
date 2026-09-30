from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "customer_id").orderBy(F.col("started_at").desc_nulls_first(), "id")
w2 = Window.partitionBy("device").orderBy(F.col("rn").desc(), "id")
w3 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_sessions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

eligible = (
    ranked
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_anti")
)

with_windows = (
    eligible
    .select(
        "id",
        "device",
        "started_at",
        "rn",
        F.first("country", ignorenulls=True).over(w3).alias("first_known_country"),
        F.last("browser", ignorenulls=True).over(w4).alias("last_known_browser"),
        F.last("country", ignorenulls=True).over(w5).alias("last_known_country"),
    )
)

ranked_sessions = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

eligible_sessions = (
    ranked_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_semi")
)

stats = (
    eligible_sessions
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .withColumn("week_start", F.date_trunc("WEEK", "started_at"))
    .groupingSets(
        [["quarter_start", "week_start", "device"], ["quarter_start", "week_start"]],
        "quarter_start",
        "week_start",
        "device",
    )
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.sort_array(F.collect_set("device")).alias("device_list_2"),
        F.min("started_at").alias("first_seen"),
        F.sort_array(F.collect_set("device")).alias("device_list_3"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    stats
    .select(
        "quarter_start",
        "week_start",
        "device",
        "device_list_2",
        "first_seen",
        "num_rows",
        "grouping_level",
    )
    .orderBy(F.col("quarter_start").desc())
)

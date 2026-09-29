from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page", "session_id").orderBy(F.col("burst_id").desc())
w4 = Window.orderBy(F.col("duration_sec_filled").desc(), "id")
w5 = Window.partitionBy("page").orderBy(F.col("event_time").asc_nulls_first())
w6 = Window.partitionBy("page").orderBy("duration_sec_filled_bucket", "id")
w7 = Window.partitionBy("page")

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-10-13").cast("date")) & F.col("page").isNotNull())
    .select(
        "*",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.when(F.col("event_type").isin(["page_view", "add_to_cart"]), "Tier 1").otherwise("Other").alias("event_type_group"),
    )
)

flagged = (
    valid
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

cleaned = (
    sessionized
    .filter(F.col("page").isNotNull())
    .select("*", F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled_2"))
)

with_windows = (
    cleaned
    .withColumn("burst_id_cume_dist", F.cume_dist().over(w3))
    .withColumn("duration_sec_filled_bucket", F.ntile(5).over(w4))
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .filter(F.hour("event_time").between(6, 14))
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec_filled",
        "event_type_group",
        "new_group_flag",
        "duration_sec_filled_2",
        "burst_id_cume_dist",
        "dedup_rank",
        F.row_number().over(w6).alias("event_seq"),
        F.max("duration_sec").over(w7).alias("page_max_duration_sec"),
    )
)

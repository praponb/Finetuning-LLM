from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy(F.col("page_length").desc_nulls_first(), "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy("burst_id", "id")

base = (
    df_web_events
    .filter(F.col("event_time") >= "2025-01-01")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("event_type").isin(["page_view", "search"]), "Group A").otherwise("Other").alias("event_type_group"),
        F.substring("event_type", 1, 4).alias("event_type_prefix"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

cleaned = (
    base
    .filter(F.col("page").isNotNull() & (F.col("event_time") >= "2025-10-05"))
    .withColumn("page_length", F.length("page"))
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "event_type_group",
        "page_length",
    )
)

flagged = (
    ranked
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w3))

latest = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "page", "duration_sec", "event_type_group", "new_group_flag")
)

result = (
    latest
    .select("id", "page", "duration_sec", "event_type_group")
    .orderBy("page")
)

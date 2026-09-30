from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at_day").desc(), "id")
w_part = Window.partitionBy("device").orderBy("started_at", "id")
w3 = Window.orderBy("started_at", "id")
w4 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
        F.when(F.col("browser").isin(["Chrome", "Firefox"]), "Group A").otherwise("Other").alias("browser_group"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "device", "started_at")
)

scored = (
    ranked
    .filter(F.col("device").isin(["desktop", "tablet"]))
    .select(
        "*",
        F.row_number().over(w_part).alias("row_seq"),
        F.row_number().over(w_part).alias("event_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
        F.row_number().over(w_part).alias("event_seq_2"),
    )
)

gaps = (
    scored
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w_part) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w4))

kept = (
    islands
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_anti")
)

remaining_keys = (
    kept
    .select("device", "id")
    .subtract(
        df_sessions
        .filter(F.col("browser").isin(["Safari", "Chrome"]))
        .select("device", "id")
    )
)

kept_rows = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "device",
        "started_at",
        "row_seq",
        "row_seq_2",
        "event_seq_2",
        "session_start_flag",
    )
    .orderBy(F.col("started_at").asc_nulls_first())
)

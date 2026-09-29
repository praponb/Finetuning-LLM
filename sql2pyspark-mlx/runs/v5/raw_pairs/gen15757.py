from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("browser").orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device")

base = (
    df_sessions
    .filter(F.col("browser") != "Edge")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.when(F.col("browser").isin(["Safari", "Edge"]), "Priority").otherwise("Other").alias("browser_group"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_type") == "purchase"), "left_semi")
)

with_windows = (
    not_flagged
    .filter(~F.col("device").isin(["tablet", "desktop"]))
    .select(
        "id",
        "device",
        "browser",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
        F.first("browser", ignorenulls=True).over(w3).alias("first_known_browser"),
        F.first("browser", ignorenulls=True).over(w4).alias("first_known_browser_2"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "browser",
        "row_seq",
        "first_known_browser_2",
        F.last("browser", ignorenulls=True).over(w5).alias("last_known_browser"),
        F.min("row_seq_2").over(w6).alias("device_min_row_seq_2"),
    )
)

not_flagged_sessions = (
    scored
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_type").isin(["purchase", "add_to_cart"])), "left_anti")
)

remaining_keys = (
    not_flagged_sessions
    .select("id")
    .intersectAll(
        df_sessions
        .filter(F.col("device") == "tablet")
        .select("id")
    )
)

reconciled = (
    not_flagged_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base_sessions = (
    reconciled
    .filter(F.col("browser").isNotNull())
    .select("*", F.round("row_seq", 1).alias("row_seq_rounded"))
)

result = (
    base_sessions
    .groupBy(
        F.when(F.col("device_min_row_seq_2") >= 6, "PREMIUM").otherwise("STANDARD").alias("device_min_row_seq_2_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("num_rows") >= 1)
)

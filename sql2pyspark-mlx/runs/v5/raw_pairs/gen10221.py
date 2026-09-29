from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc_nulls_last(), "id")
w_recent = Window.partitionBy("device").orderBy(F.col("rank_in_group").desc(), "id")
w3 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("browser", "device").orderBy(F.col("started_at").desc(), "id")
w6 = Window.partitionBy("browser", "country").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & F.col("country").isin(["UK", "TH"]))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("browser").isin(["Firefox", "Edge"]), "Tier 1").otherwise("Other").alias("browser_group"),
        F.substring("browser", 1, 3).alias("browser_prefix"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_type").isin(["add_to_cart", "purchase", "search"])), "left_anti")
)

deduped = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "device",
        F.row_number().over(w_recent).alias("seq_no"),
        F.first("browser", ignorenulls=True).over(w3).alias("first_known_browser"),
        F.first("browser", ignorenulls=True).over(w4).alias("first_known_browser_2"),
        F.row_number().over(w5).alias("event_seq"),
        F.last("browser", ignorenulls=True).over(w6).alias("last_known_browser"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < "2024-10-12"), "left_anti")
)

eligible_sessions = (
    kept
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id") == 35), "left_anti")
)

result = (
    eligible_sessions
    .select("id", "device", "seq_no", "first_known_browser", "event_seq")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "country").orderBy(F.col("started_at").asc_nulls_first(), "id")

deduped = (
    df_sessions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "device")
)

kept_keys = (
    deduped
    .select("device", "id")
    .intersect(
        df_sessions
        .filter(F.col("browser").isin(["Firefox", "Chrome", "Edge"]))
        .select("device", "id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device") == F.col("k.device")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id").isin([7, 13, 25])), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.session_id").isin([1, 14, 21])), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.page").isNotNull()), "left_anti")
)

matched_keys = (
    eligible
    .select("id", "device")
    .subtract(
        df_sessions
        .filter(F.col("customer_id").isin([37, 39]))
        .select("id", "device")
    )
)

kept_rows_sessions = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    kept_rows_sessions
    .groupBy("device")
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows_2"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
    )
    .orderBy(F.col("num_devices").desc())
)

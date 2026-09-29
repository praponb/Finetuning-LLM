from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("page").eqNullSafe(F.lit("/home"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.length("event_type").alias("event_type_length"),
    )
)

kept_keys = (
    base
    .select("event_type", "page")
    .intersect(
        df_web_events
        .filter(F.col("duration_sec") < 332)
        .select("event_type", "page")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("duration_sec") < 292)
    .select(
        "*",
        F.when(F.col("page") == "/checkout", 1).otherwise(0).alias("is_checkout"),
        (F.col("duration_sec") - F.col("event_type_length")).alias("duration_sec_minus_event_type_length"),
    )
)

kept_keys_2 = (
    cleaned
    .select("event_type", "page")
    .subtract(
        df_web_events
        .filter(F.col("page").isin(["/home", "/search"]))
        .select("event_type", "page")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys_2.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "id",
        "session_id",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("event_type_sum_duration_sec"),
        F.sum("event_type_length").over(w).alias("event_type_sum_event_type_length"),
    )
)

result = (
    metrics
    .groupBy(
        "session_id",
        F.when(F.col("duration_sec") >= 500, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
        F.when(F.col("event_type_sum_event_type_length") >= 443, "HIGH").otherwise("LOW").alias("event_type_sum_event_type_length_tier"),
    )
    .agg(
        F.sum("event_type_sum_duration_sec").alias("total_event_type_sum_duration_sec"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
    )
    .orderBy(F.col("total_event_type_sum_duration_sec").desc_nulls_first())
)

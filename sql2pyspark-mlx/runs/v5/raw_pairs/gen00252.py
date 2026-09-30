from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host", "service").orderBy(F.col("session_id").desc())
w4 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("service", "k")
w6 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

gaps = (
    flattened
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = (
    gaps
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "k",
        "attr_value",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

matched_keys = (
    sessions_app_logs
    .select("service")
    .subtract(
        df_app_logs
        .filter(F.hour("log_ts").between(11, 14))
        .select("service")
    )
)

reconciled = (
    sessions_app_logs
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "id",
        "host",
        "log_ts",
        "k",
        "attr_value",
        "session_id",
        F.cume_dist().over(w3).alias("session_id_cume_dist"),
        F.sum("session_id").over(w4).alias("session_id_sum_15m"),
        F.sum("session_id").over(w5).alias("service_sum_session_id"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "log_ts",
        "k",
        "attr_value",
        "session_id_cume_dist",
        "session_id_sum_15m",
        F.sum("session_id").over(w6).alias("session_id_sum_2h"),
    )
)

cleaned = (
    enriched
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "log_ts",
        "k",
        "session_id_cume_dist",
        "session_id_sum_2h",
        F.length("k").alias("k_length"),
    )
)

result = cleaned.select("id", "session_id_sum_2h", "k_length")

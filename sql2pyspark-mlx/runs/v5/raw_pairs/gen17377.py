from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    exploded
    .filter(F.col("status_code") < 324)
    .select(
        "log_ts",
        "status_code",
        "attr_value",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

scored = (
    parsed
    .select(
        "log_ts",
        "status_code",
        F.count("status_code_high").over(w).alias("status_code_high_count_15m"),
    )
)

cleaned = (
    scored
    .filter(F.col("status_code").between(358, 405))
    .select(
        "*",
        F.floor("status_code_high_count_15m").alias("status_code_high_count_15m_whole"),
    )
)

result = (
    cleaned
    .select(
        "log_ts",
        "status_code",
        "status_code_high_count_15m",
        "status_code_high_count_15m_whole",
    )
    .orderBy(F.col("log_ts").desc_nulls_first())
)

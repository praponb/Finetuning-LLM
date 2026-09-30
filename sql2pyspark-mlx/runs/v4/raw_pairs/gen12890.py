from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.lead("status_code", 1).over(w).alias("next_status_code"),
        F.count("latency_ms").over(w2).alias("latency_ms_count_15m"),
    )
)

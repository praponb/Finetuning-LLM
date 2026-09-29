from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("status_code")
w2 = Window.partitionBy("host").orderBy("status_code", "id", "key")
w3 = Window.orderBy("status_code", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

scored = (
    flattened
    .select(
        "*",
        F.percent_rank().over(w).alias("status_code_percent_rank"),
        F.ntile(3).over(w2).alias("status_code_bucket"),
        F.lag("status_code", 2, 0).over(w3).alias("prev_status_code"),
    )
)

kept_keys = (
    scored
    .select("host", "level")
    .subtract(
        df_app_logs
        .filter(~F.col("level").isin(["DEBUG", "INFO"]))
        .select("host", "level")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

daily = (
    verified
    .groupBy("host")
    .agg(
        F.count_if(F.col("host") == "h2").alias("h2_events"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.avg(F.when(F.col("level") == "DEBUG", F.col("status_code_percent_rank"))).alias("debug_avg_status_code_percent_rank"),
    )
)

result = (
    daily
    .select("host", "h2_events", "debug_count")
    .orderBy(F.col("debug_count").asc_nulls_first())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("status_code").desc(), "id", "key")
w2 = Window.partitionBy("level").orderBy("status_code")
w3 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w4 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

latest = (
    exploded
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    latest
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

gaps = (
    ranked
    .withColumn("is_new_session", F.when(F.col("log_ts") - F.lag("log_ts").over(w3) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_app_logs = gaps.withColumn("island_id", F.sum("is_new_session").over(w4))

result = (
    sessions_app_logs
    .cube(
        F.when(F.col("island_id") >= 9, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.countDistinct("key").alias("distinct_keys"),
        F.min("island_id").alias("min_island_id"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
        F.percentile_approx("status_code", [0.25, 0.5, 0.75]).alias("status_code_quartiles"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
    )
    .orderBy(F.col("h1_rows").desc())
)

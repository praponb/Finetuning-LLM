from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("log_ts").desc())
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "key")
w3 = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("level").orderBy("latest_rank", "id", "key")
w5 = Window.partitionBy("key", "level").orderBy("next_island_id", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

deduped = (
    flattened
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

gaps = (
    deduped
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w2) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "key",
        "attr_value",
        "latest_rank",
        F.sum("is_new_burst").over(w3).alias("island_id"),
    )
)

metrics = (
    sessionized
    .select(
        "*",
        F.lead("island_id").over(w_hist).alias("next_island_id"),
        F.lag("island_id").over(w_hist).alias("prev_island_id"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

deduped_app_logs = (
    metrics
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    deduped_app_logs
    .select(
        "id",
        "host",
        "level",
        "key",
        "latest_rank",
        "island_id",
        "next_island_id",
        "row_seq",
        F.when(F.col("key").rlike("^(trace|version)$"), "EXTENDED").when(F.col("key").rlike("^(region)$"), "GROUP_B").otherwise("GROUP_A").alias("key_zone"),
    )
)

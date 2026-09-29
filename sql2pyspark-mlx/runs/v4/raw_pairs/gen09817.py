from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("created_at", "id", "key")
w2 = Window.partitionBy("key").orderBy("created_at", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("key").orderBy(F.col("session_start_flag").asc_nulls_first(), "id", "key")
w4 = Window.partitionBy("key").orderBy("session_start_flag", "id", "key")
w5 = Window.partitionBy("key").orderBy(F.col("burst_id").desc())
w6 = Window.orderBy(F.col("burst_id").desc())
w7 = Window.orderBy("session_start_flag", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = df_carts.select("id", "created_at", F.explode("attributes").alias("key", "value"))

flagged = (
    lines
    .withColumn("session_start_flag", F.when(F.lag("created_at").over(w).isNull() | (F.col("created_at") > F.lag("created_at").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

valid = (
    islands
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-02"))) & F.col("key").isNotNull() & (F.col("created_at") < "2025-01-06"))
    .select(
        "id",
        "created_at",
        "key",
        "value",
        "session_start_flag",
        "burst_id",
        F.upper("key").alias("key_upper"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .withColumn("prev_burst_id", F.lag("burst_id", 1).over(w4))
    .withColumn("burst_id_percent_rank", F.percent_rank().over(w5))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("burst_id_percent_rank_2", F.percent_rank().over(w6))
    .withColumn("last_known_session_start_flag", F.last("session_start_flag", ignorenulls=True).over(w7))
)

result = (
    metrics
    .select(
        "id",
        "key",
        "value",
        "session_start_flag",
        "burst_id",
        "key_upper",
        "rn",
        "prev_burst_id",
        "burst_id_percent_rank",
        F.when(F.col("key").rlike("^(device|utm_source)$"), "GROUP_A").otherwise("EXTENDED").alias("key_zone"),
    )
    .orderBy(F.col("burst_id_percent_rank").asc_nulls_last())
)

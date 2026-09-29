from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("started_at_date").asc_nulls_last(), "id")

valid = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-12-08 17:00:00").cast("timestamp"))
    .select(
        "*",
        F.date_trunc("month", "started_at").alias("started_at_period"),
        F.to_date("started_at").alias("started_at_date"),
    )
)

flagged = (
    valid
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

deduped = (
    sessions_sessions
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

summary = (
    deduped
    .groupBy(
        F.when(F.col("session_id") >= 7, "HIGH").otherwise("LOW").alias("session_id_tier"),
        "browser",
    )
    .agg(
        F.sum(F.when(F.col("device") == "mobile", F.col("session_id"))).alias("mobile_total_session_id"),
        F.min("started_at_period").alias("first_seen"),
        F.max(F.when(F.col("browser") == "Edge", F.col("session_start_flag"))).alias("edge_max_session_start_flag"),
        F.size(F.collect_set("country")).alias("num_countries"),
    )
    .filter(F.col("num_countries") < 1)
)

result = (
    summary
    .select(
        "session_id_tier",
        "browser",
        "edge_max_session_start_flag",
        "num_countries",
    )
    .orderBy("session_id_tier")
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc_nulls_last(), "id")
w2 = Window.partitionBy("device").orderBy(F.col("latest_rank").desc(), "id")
w3 = Window.partitionBy("country").orderBy(F.col("latest_rank").desc(), "id")
w4 = Window.partitionBy("country").orderBy("latest_rank", "id")
w5 = Window.partitionBy("country").orderBy("started_at", "id")
w6 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-06").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("country_lower", F.lower("country"))
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped_sessions = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "started_at", "country", "latest_rank")
)

with_windows = (
    deduped_sessions
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

flagged = (
    with_windows
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w5) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w6))

result = (
    sessionized
    .select(
        "id",
        "started_at",
        "country",
        "latest_rank",
        "row_seq",
        "event_seq",
        "is_new_session",
        "session_no",
    )
    .orderBy(F.col("row_seq").desc())
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec_adjusted").asc_nulls_first(), "id")

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_time") < "2023-05-01"))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.round((F.col("duration_sec") * 1.15), 2).alias("duration_sec_adjusted"),
    )
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

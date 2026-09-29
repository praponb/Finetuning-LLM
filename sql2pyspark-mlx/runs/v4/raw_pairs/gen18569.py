from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id")

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
        F.coalesce("page", F.lit("NONE")).alias("resolved_page"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

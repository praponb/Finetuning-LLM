from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc_nulls_last(), "id")

base = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("duration_sec_rounded", F.round("duration_sec", 1))
    .withColumn("resolved_page", F.coalesce("page", F.lit("OTHER")))
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

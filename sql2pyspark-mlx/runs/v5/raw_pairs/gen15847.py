from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_last(), "id")

deduped = (
    df_web_events
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "event_type", "event_time", "duration_sec")
)

result = (
    deduped
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.when(F.col("event_type").rlike("^(page_view|click)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("event_type_zone"),
    )
    .orderBy(F.col("duration_sec").desc_nulls_last())
)

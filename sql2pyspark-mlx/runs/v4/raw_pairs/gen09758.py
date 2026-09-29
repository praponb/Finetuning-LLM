from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_first(), "id")

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("session_id").isin([19, 38]))
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

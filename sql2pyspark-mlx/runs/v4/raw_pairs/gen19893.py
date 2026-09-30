from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").asc_nulls_first())
w2 = Window.orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("page").orderBy("duration_sec", "id")
w4 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

metrics = (
    df_web_events
    .withColumn("duration_sec_rank", F.rank().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("duration_sec_bucket", F.ntile(4).over(w3))
    .withColumn("duration_sec_sum_90d", F.sum("duration_sec").over(w4))
)

result = (
    metrics
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec_rank",
        "event_seq",
        "duration_sec_bucket",
        "duration_sec_sum_90d",
    )
    .orderBy(F.col("session_id").desc_nulls_last())
)

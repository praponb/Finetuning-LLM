from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type", "session_id").orderBy(F.col("duration_sec").desc_nulls_last())
w5 = Window.partitionBy("event_type", "page").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .withColumn("duration_sec_sum_90d", F.sum("duration_sec").over(w))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w2))
    .withColumn("last_known_page", F.last("page", ignorenulls=True).over(w3))
    .withColumn("duration_sec_rank", F.rank().over(w4))
    .withColumn("duration_sec_bucket", F.ntile(3).over(w5))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy(F.col("duration_sec").asc_nulls_first())

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("clean_event_type", F.trim("event_type"))
)

result = (
    cleaned
    .withColumn("duration_sec_sum_7d", F.sum("duration_sec").over(w))
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w2))
)

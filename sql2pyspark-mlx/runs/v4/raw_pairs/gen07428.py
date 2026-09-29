from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2023-06-25 06:00:00"))) & F.col("page").isNotNull())
    .withColumn("event_time_hour", F.hour("event_time"))
    .withColumn("is_cart", F.when(F.col("page") == "/cart", 1).otherwise(0))
)

result = (
    filtered
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w_running))
    .withColumn("event_time_hour_avg_3", F.avg("event_time_hour").over(w2))
)

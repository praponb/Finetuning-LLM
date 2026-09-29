from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id").rowsBetween(-6, -1)

result = (
    df_web_events
    .withColumn("duration_sec_bucket", F.ntile(10).over(w))
    .withColumn("duration_sec_avg_6_prior", F.avg("duration_sec").over(w2))
)

from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("duration_sec").isNull())
    .select("id", "session_id", "duration_sec", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "session_id", "duration_sec")
    .orderBy(F.col("id").desc(), "duration_sec")
)

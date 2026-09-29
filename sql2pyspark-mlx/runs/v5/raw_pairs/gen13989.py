from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        "page",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("duration_sec").desc(), F.col("session_id").desc())
)

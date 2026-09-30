from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .select("id", "page", "duration_sec", F.rank().over(w).alias("duration_sec_rank"))
    .orderBy("duration_sec")
)

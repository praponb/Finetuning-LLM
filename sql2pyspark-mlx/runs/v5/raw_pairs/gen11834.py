from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec")

result = (
    df_web_events
    .select("id", "page", "duration_sec", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("duration_sec").desc())
)

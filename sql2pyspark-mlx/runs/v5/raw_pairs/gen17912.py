from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "search", "purchase"]))
    .select("id", "page", "duration_sec", F.row_number().over(w).alias("row_num"))
    .orderBy(F.col("row_num").desc())
)
